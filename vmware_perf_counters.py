#!/usr/bin/env python3
import ssl
from dataclasses import dataclass
from matplotlib.ticker import ScalarFormatter
from pyVim.connect import Disconnect, SmartStubAdapter
from pyVmomi import vim
import datetime
import matplotlib.pyplot as plt
import argparse
import questionary
from questionary import Choice
from requests import Session
from zeep import Client, Settings
from zeep.transports import Transport
from pathlib import Path


@dataclass
class CounterInfo:
    id: int
    name: str
    rollup_type: str
    label: str
    summary: str


def enable_guest(addr: str, user: str, password: str):
    session = Session()
    session.verify = False
    transport = Transport(session=session)
    settings = Settings(strict=False, xml_huge_tree=True)

    # load WSDL from disk
    wsdl_path = Path(__file__).parent / "wsdl/vimService.wsdl"
    client = Client(wsdl=wsdl_path.as_uri(),
                    transport=transport,
                    settings=settings)

    service = client.bind("VimService", "VimPort")
    service._binding_options["address"] = f"https://{addr}/sdk"

    # log in
    mir = client.get_type("ns0:ManagedObjectReference")
    sm_ref = mir("SessionManager", type="SessionManager")
    service.Login(_this=sm_ref,
                  userName=user,
                  password=password)

    si_ref = mir("ServiceInstance", type="ServiceInstance")
    sc = service.RetrieveServiceContent(_this=si_ref)
    perf_mgr_ref = sc.perfManager
    print("Type:", perf_mgr_ref.type)
    print("Value:", perf_mgr_ref._value_1)

    # call QueryPerfCounterInt
    counters = service.QueryPerfCounterInt(_this=perf_mgr_ref)
    mappings = []
    result = []
    clm = client.get_type("ns0:PerformanceManagerCounterLevelMapping")
    for c in counters:
        mappings.append(clm(counterId=c.key, aggregateLevel=4))
        result.append(CounterInfo(c.key, f'{c.groupInfo.key}.{c.nameInfo.key}', c.rollupType, c.unitInfo.label, c.nameInfo.summary))

    # call updateCounterLevelMapping
    service.UpdateCounterLevelMapping(_this=perf_mgr_ref, counterLevelMap=mappings)
    session_cookie_value = client.transport.session.cookies.get('vmware_soap_session')
    return session_cookie_value, result


def get_entities(content, type: vim.ManagedEntity):
    view = content.viewManager.CreateContainerView(
        content.rootFolder, [type], recursive=True
    )
    entities = view.view
    view.Destroy()
    return entities


def get_counters(perf_mgr, host, interval_id) -> list[CounterInfo]:
    available = perf_mgr.QueryAvailablePerfMetric(entity=host, intervalId=interval_id)
    counters = []
    counter_info = {c.key: c for c in perf_mgr.perfCounter}
    for m in available:
        c = counter_info.get(m.counterId)
        if c:
            full_name = f"{c.groupInfo.key}.{c.nameInfo.key}"
            counters.append(CounterInfo(m.counterId, full_name, c.rollupType, c.unitInfo.label, c.nameInfo.summary))
    return counters


def seconds_to_readable(seconds):
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if sec: parts.append(f"{sec}s")
    return ' '.join(parts) or '0s'


def get_intervals(perf_mgr):
    intervals = []
    intervals.append((20, f'  ID: 20 seconds (Realtime) - Level 1, Retained for {seconds_to_readable(3600)}'))
    for i in perf_mgr.historicalInterval:
        readable = seconds_to_readable(i.length)
        intervals.append((i.samplingPeriod, f"  ID: {i.samplingPeriod}s ({i.name}) - Level {i.level}, Retained for {readable}"))
    return intervals


def get_args():
    parser = argparse.ArgumentParser(description="build performance counter graph")
    parser.add_argument('-s', '--host', required=True, help='vCenter server hostname or IP')
    parser.add_argument('-u', '--user', required=True, help='Username for vCenter')
    parser.add_argument('-p', '--password', required=True, help='Password for vCenter')
    return parser.parse_args()


def main():
    args = get_args()

    session_cookie_value, guest_metrics = enable_guest(args.host, args.user, args.password)

    # build pyVmomi stub and inject cookie ---
    ssl_context = ssl._create_unverified_context()
    stub = SmartStubAdapter(host=args.host, sslContext=ssl_context)
    stub.cookie = f"vmware_soap_session={session_cookie_value}"
    si = vim.ServiceInstance("ServiceInstance", stub)

    # 1) pick MO type
    type = questionary.select(
        "Pick a type:",
        choices=[
            Choice(title='HostSystem', value=vim.HostSystem),
            Choice(title='VirtualMachine', value=vim.VirtualMachine),
            Choice(title='Datastore', value=vim.Datastore),
        ]
    ).ask()

    content = si.RetrieveContent()

    # 2) pick MO
    mo = questionary.select(
        "Pick an object:",
        choices=[Choice(title=h.name, value=h) for h in get_entities(content, type)]
    ).ask()

    perf_mgr = content.perfManager

    # 3) pick pef interval
    interval = questionary.select("Pick a performance interval:",
                                  choices=[Choice(title=name, value=id) for id, name in get_intervals(perf_mgr)]
                                  ).ask()

    mask = questionary.text("filter metrics by class (e.g. 'net' fo metrics like net.usage.average)").ask() or None

    # 4) pick performance counter
    all_counters = [*get_counters(perf_mgr, mo, interval), *guest_metrics]
    filtered_counters = [t for t in all_counters if t.name.casefold().startswith(mask)]
    counter = questionary.select(
        "Select metric to query:",
        choices=[Choice(title=f'{counter.name}.{counter.rollup_type} ({counter.id})', value=counter) for counter in
                 filtered_counters]
    ).ask()

    # 5) query performance counter
    if interval != 20:
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(days=365)
    else:
        end_time = None
        start_time = None

    metric_id = vim.PerformanceManager.MetricId(counterId=counter.id, instance="*")
    spec = vim.PerformanceManager.QuerySpec(
        entity=mo,
        metricId=[metric_id],
        startTime=start_time,
        endTime=end_time,
        intervalId=interval
    )

    perf_data = perf_mgr.QueryPerf(querySpec=[spec])
    if not perf_data:
        print(f"No data returned for {counter.name}.")
        Disconnect(si)
        return

    # 6) build graph
    rec = perf_data[0]
    timestamps = [si_entry.timestamp for si_entry in rec.sampleInfo]

    fig, ax1 = plt.subplots()

    sf = ScalarFormatter(useMathText=True)
    sf.set_scientific(True)
    sf.set_powerlimits((0, 0))
    sf.set_useOffset(False)
    ax1.yaxis.set_major_formatter(sf)

    if counter.name == 'cpu.ready':
        ax2 = ax1.twinx()
        ax2.set_ylabel(f'{counter.name} %')
        ax2.tick_params(axis='y', labelcolor='red')

    for m in rec.value:
        values = m.value
        instance_label = m.id.instance or "(aggregate)"
        ax1.plot(timestamps, values, label=instance_label)
        if counter.name == 'cpu.ready':
            ax2.plot(timestamps, [x / (interval * 10) for x in values], label=instance_label)

    plt.xlabel("Time (UTC)")
    ax1.set_ylabel(f'{counter.name} ({counter.label})')
    ax1.tick_params(axis='y', labelcolor='blue')
    plt.title(f"{counter.name} on {mo.name}\n{counter.summary}")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.grid()
    plt.show()

    Disconnect(si)


if __name__ == '__main__':
    main()

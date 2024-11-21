"""
Fetch the serial numbers of all optical modules deployed into the switches 
and save them into a CSV file.
"""

import argparse
import csv
from datetime import datetime

from cloudvision.Connector.codec.custom_types import FrozenDict
from cloudvision.Connector.grpc_client import GRPCClient, create_query

FILE_CSV = "transceivers.csv"


def parse_arguments():
    """Parse the command line arguments
    
    :return: The parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Login to CVP via username and password and generate an access token.")
    parser.add_argument("--servername", required=True, help="The server name to connect to")
    parser.add_argument("--tokenfile",
                        required=True,
                        help="The file name containing the access token")
    parser.add_argument("--cafile",
                        required=False,
                        help="The file name containing the self-signed CA certificate")
    parser.add_argument("--csvfile",
                        required=False,
                        help="The output file")

    return parser.parse_args()


def get_client(api_server_address, token=None, certs=None, key=None, ca=None):
    """Return a GRPC client to the specified server"""
    return GRPCClient(f"{api_server_address}:443", token=token, key=key, ca=ca, certs=certs)


def get(client, dataset, path_elements, keys=[], start=None, end=None, versions=None, sharding=None):
    """Run a DB query and return the result"""
    result = {}

    query = [
        create_query([(path_elements, keys)], dataset)
    ]

    for batch in client.get(query, start, end, versions, sharding):
        for notif in batch["notifications"]:
            if start or end or versions:
                # We expect multiple reponses
                result.update({notif["timestamp"].ToMilliseconds(): notif["updates"]})
            else:
                # We expect only a single reponse
                result.update(notif["updates"])
    return result


def get_multiple(client, dataset, path_key_sets, start=None, end=None, versions=None, sharding=None):
    """Run a DB query and return the result (supporting multiple paths)"""
    result = {}

    query = [
        create_query(path_key_sets, dataset)
    ]

    for batch in client.get(query, start, end, versions, sharding):
        for notif in batch["notifications"]:
            if start or end or versions:
                path = "/".join(notif["path_elements"])
                if path not in result:
                    result[path] = {}
                result[path].update({notif["timestamp"].ToMilliseconds(): notif["updates"]})
            else:
                result.update(notif["updates"])
    return result


def get_transceiver_info(client, device):
    """Get the transceiver info"""
    keys = [
        "actualIdEepromContents"
    ]
    interfaces = list(get_interfaces(client, device).keys())
    path_key_sets = []
    for interface in interfaces:
        path_elements = [
            "Sysdb",
            "hardware",
            "archer",
            "xcvr",
            "status",
            "all",
            interface
        ]
        path_key_sets.append((path_elements, keys))

    dataset = device

    return get_multiple(client, dataset, path_key_sets, versions=100000)


def get_devices(client):
    """Get the list of devices"""
    path_elements = [
        "DatasetInfo",
        "Devices"
    ]
    dataset = "analytics"
    return get(client, dataset, path_elements)


def get_interfaces(client, device):
    """Get the list of interfaces"""
    path_elements = [
        "Sysdb",
        "hardware",
        "archer",
        "xcvr",
        "status",
        "all"
    ]
    dataset = device
    return get(client, dataset, path_elements)


def decode_transceiver_info(data):
    """Decode the transceiver info"""
    result = {}
    try:
        for path in data.keys():
            lastres = {}
            interface = path.split('/')[-1]
            result[interface] = {}
            updates = data[path]
            timestamps = sorted(list(updates.keys()))
            for timestamp in timestamps:
                update = updates[timestamp].get("actualIdEepromContents", {})
                if "vendorPartNum" in update.keys():
                    action = "Inserted"
                    sku = update.get("vendorPartNum").strip()
                    serial = update.get("vendorSerialNum").strip()
                else:
                    action = "Removed"
                    sku = lastres.get("sku")
                    serial = lastres.get("serial")
                res = {"action": action, "sku": sku, "serial": serial}
                if res == lastres:
                    # No change
                    continue
                if serial:
                    if serial not in result[interface].keys():
                        result[interface][serial] = []
                    lastres = {
                        "action": res['action'],
                        "sku": res['sku'],
                        "serial": res['serial']}
                    res["timestamp"] = timestamp
                    result[interface][serial].append(res)
    except Exception:
        pass
    return result


def write_transceiver_info(devices, output_file):
    """Write the transceiver info to a CSV file"""
    device_list = []

    for device_serial in list(devices.keys()):
        device_hostname = devices[device_serial].get("hostname")
        transceiver_info = devices[device_serial].get("transceiverInfo", {})

        for interface in transceiver_info.keys():
            for transceiver_serial in transceiver_info[interface].keys():
                for event in transceiver_info[interface][transceiver_serial]:
                    device_list.append({
                        "device_serial": device_serial,
                        "device_hostname": device_hostname,
                        "interface": interface,
                        "transceiver_serial": transceiver_serial,
                        "sku": event['sku']
                    })

    with open(output_file, mode="w", encoding="utf-8") as file:
        writer = csv.DictWriter(file,
                                fieldnames=["device_serial",
                                            "device_hostname",
                                            "interface",
                                            "transceiver_serial",
                                            "sku"])
        writer.writeheader()
        writer.writerows(device_list)


def unfreeze(o):
    """Unfreeze the object"""
    if isinstance(o, (dict, FrozenDict)):
        return dict({k: unfreeze(v) for k, v in o.items()})

    if isinstance(o, (str)):
        return o

    try:
        return [unfreeze(i) for i in o]
    except TypeError:
        pass

    return o


def convert_timestamp(timestamp):
    """Convert the timestamp to a human readable format"""
    return datetime.utcfromtimestamp(timestamp / 1e3).isoformat(timespec='milliseconds')


def main():
    """Main function"""
    args = parse_arguments()

    ca_file = args.cafile if args.cafile else None
    output_file = args.csvfile if args.csvfile else FILE_CSV

    client = get_client(args.servername, token=args.tokenfile, ca=ca_file)
    devices = unfreeze(get_devices(client))

    for device in devices:
        devices[device]["transceiverInfo"] = decode_transceiver_info(get_transceiver_info(client, device))

    write_transceiver_info(devices, output_file)


if __name__ == "__main__":
    main()

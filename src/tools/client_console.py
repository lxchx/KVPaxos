from src.client.client import KVClient
from src.proto import KVService_pb2
from src.utils.log_manager import LogManager

log_manager = LogManager(__name__)
logger = log_manager.get_logger()

members = ['127.0.0.1:8001', '127.0.0.1:8002', '127.0.0.1:8003']
members = [(m, i+1) for i, m in enumerate(members)]

members_info = KVService_pb2.MembersInfo()
for addr, id in members:
    members_info.members.append(KVService_pb2.Member(addr=addr, id=id))
members_info.version = 3

client = KVClient(members_info)

while True:
    cmd = input('Enter a command: ')
    cmd_parts = cmd.split(' ')
    if cmd_parts[0] == 'get':
        if len(cmd_parts) != 2:
            print('Usage: get key')
        else:
            key = cmd_parts[1]
            value = client.Get(key)
            if value is None:
                print('Key not found')
            else:
                print(value)
    elif cmd_parts[0] == 'set':
        if len(cmd_parts) != 3:
            print('Usage: set key value')
        else:
            key, value = cmd_parts[1], cmd_parts[2]
            try:
                client.Set(key, value)
            except:
                print('Set failed')
            else:
                print('Set successfully')
    elif cmd_parts[0] == 'addmember':
        if len(cmd_parts) != 2:
            print('Usage: addmember member1_addr,member2_addr,...')
        else:
            members = cmd_parts[1].split(',')
            print(f'members: {members}')
            ok = client.AddMember(members)
            if ok:
                print('Add members successfully')
            else:
                print('Add members failed')
    elif cmd_parts[0] == 'deletemember':
        if len(cmd_parts) != 2:
            print('Usage: deletemember member1_id,member2_id,...')
        else:
            members = cmd_parts[1].split(',')
            members = [int(e) for e in members]
            print(f'members: {members}')
            ok = client.DeleteMember(members)
            if ok:
                print('Delete members successfully')
            else:
                print('Delete members failed')
    elif cmd_parts[0] == 'exit':
        break
    else:
        print('Unknown command')

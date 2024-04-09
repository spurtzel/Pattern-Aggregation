import csv
import datetime
import json
import socket
import subprocess
import sys
import threading
import time
import os 

event_types_sent = []
number_of_sent_messages = 0
eof_sent = []
aggregates_computed  = []

my_dicts = {}

def send_events(csv_file_path, forwarding,myid,processing): #  can be called multiple times
    global event_types_sent
    global number_of_sent_messages
    global eot_sent
    global my_dicts
    
    locally_generated_inputs = {}
    my_aggregates = [forwarding[x][1] for x in forwarding.keys()]
    for aggregate in my_aggregates:
        locally_generated_inputs[aggregate] = [x for x in forwarding.keys() if aggregate == forwarding[x][1]]
        
    #print(myid," SEND CALLED")    
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
                if len(row) == 2:
                    event_type, timestamp = row
                    message = f"{event_type} {timestamp} 1 {event_type}" 
                else:
                    event_type, timestamp, count, rate = row
                    message = f"{event_type} {timestamp} {count} {rate}"

                if not event_type in event_types_sent:
                    event_types_sent.append(event_type) # THIS IS REQUIRED FOR EOF MESSAGES                
                    
                if event_type in forwarding:                
                    node_ids = forwarding[event_type][0]
                    for node_id in node_ids:
                        message_bytes = message.encode('utf-8')
                        port = 61000 + node_id
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                            if myid != node_id:
                                number_of_sent_messages += 1
                                sock.sendto(message_bytes, ("localhost", port))
                            else: #write local events directly to dict
                                myfields = message.split(" ")
                                count = myfields[2]
                                rate = myfields[3]
                                aggregate = forwarding[event_type][1]
                                if  int(timestamp) in my_dicts[aggregate].keys():
                                    my_dicts[aggregate][int(timestamp)][2] += int(count)
                                else:
                                    my_dicts[aggregate][int(timestamp)] = [event_type, int(timestamp), int(count),rate]
                            
                          
        # send one message per node per aggregate
        # iterate over aggregates
        # send to nodes for aggregate
        my_eof_messages = {}
        for aggregate in my_aggregates:
            
                my_eof_messages[aggregate] = []
                for key in forwarding.keys():
                    if aggregate == forwarding[key][1]:
                        for node_id in forwarding[key][0] :
                            if not node_id in my_eof_messages[aggregate]:
                                    my_eof_messages[aggregate].append(node_id)
        
        
        # send only eof to aggregate source if not locally processed events also contribute to aggregate, and have not been processed yet
        if os.path.getsize(csv_file_path) == 0:
            event_types_sent.append(csv_file_path.split("_")[1])
        for aggregate in  my_eof_messages.keys():
            # aggregate file empty
            if set(locally_generated_inputs[aggregate]).issubset(set(event_types_sent)) and not aggregate in eof_sent:
                eof_sent.append(aggregate)
                for node_id in my_eof_messages[aggregate]: 
                        message = aggregate + " END-OF-STREAM"    # message is tuple (Aggregate, END-OF-STREAM)
                        message_bytes = message.encode('utf-8')
                        port = 61000 + node_id
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                            print(myid," sending ", message , " to ", node_id)
                            sock.sendto(message_bytes, ("localhost", port))
        if not processing:
            print(myid,"has sent",number_of_sent_messages,"messages in total")                    

def only_source(processing):
    if not processing:
        return True

def get_aggregate(eventtype, processing): # get the aggregate a respective event type is input of
    for aggregate in processing:
        if eventtype in processing[aggregate][2]:
            return aggregate

def listen_to_port(myid, processing,forwarding): # myid used to setup listening port, processing for counting eof messages etc, forwarding probably used to process output of summary
    global my_dicts 
    global aggregates_computed 
    all_aggregates = []
    if only_source(processing): # if node is only source, stop listening -> nothing to be done after sending primitive event stream initially
        return
    else:
        eof_counter = {} #  eof_counter is a dict of dicts, each aggregate has a value: number of eofs received
        predecessors = {} # predecessors is a dict of dict, each aggregate has a value: number of predecessors for this aggregate
        
        for aggregate in processing.keys(): # instantiate dict for each aggregate for the respective events input to the aggregate
            eof_counter[aggregate] = 0 # count eof messages per aggregate to start matching after everything is received
            predecessors[aggregate] = processing[aggregate][3] # predecessors per aggregate used for counting eofs


    port = 61000 + myid   
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    sock.bind(('', port))
    
    print(f"Listening on port {port}...")
    try:
        while True:
            # Wait for a message
            message, address = sock.recvfrom(1024)              
            # Decode the message
            decoded_message = message.decode('utf-8')          
            myfields = decoded_message.split(" ")
            
            if  myfields[1] == "END-OF-STREAM":
               
               if myfields[0] in eof_counter.keys():
                    eof_counter[myfields[0]] += 1 #decoded_message[0] is aggregate
               
               for aggregate in processing.keys():
                   if not aggregate in all_aggregates and eof_counter[aggregate] == predecessors[aggregate]:                       
                       all_aggregates.append(aggregate)
                       
               if len(processing.keys()) == len(all_aggregates):
                       print(myid,"has sent",number_of_sent_messages,"messages in total")
                       
               for aggregate in processing.keys():      
                   if eof_counter[aggregate] == predecessors[aggregate] and not aggregate in aggregates_computed: # if eof counter reached for current aggegate (decoded_message[0]) start processing the events in my_dicts[aggregate]
                        print(myid, "AGGREGATE: ", aggregate)
                        aggregates_computed.append(aggregate)
                        received_events = []
    
                        for event in sorted(my_dicts[aggregate].keys()):  # sorted by timestamps
                            # if message contains a projection aggregate
                            if len(my_dicts[aggregate][event][0]) > 1:
                                # swap the first and last elements
                                my_dicts[aggregate][event][0], my_dicts[aggregate][event][-1] = my_dicts[aggregate][event][-1], my_dicts[aggregate][event][0]
                            received_events.extend(map(str, my_dicts[aggregate][event]))
    
                        # join the accumulated content into a single stream/string with spaces
                        input_stream = ' '.join(received_events)
                       # print(myid, aggregate, " input_stream:",input_stream) 
                        executable_path = "./sequence_accumulator_fat"
                        sequence_pattern = aggregate
                        output_eventtype = processing[aggregate][1]
    
                        options = ["-t", output_eventtype, sequence_pattern]
    
                        command = [executable_path] + options
    
                        process = subprocess.run(command, input=input_stream, text=True, capture_output=True)
    
                        output_stream = process.stdout
    
                        elements = output_stream.split()
    
                        groups = [elements[i:i+4] for i in range(0, len(elements), 4)]
                        with open('traces/trace_' +aggregate+"_"+ str(myid) + '.csv', 'w') as f:
                            for group in groups:
                                group = [group[3],group[1],group[2],group[0]]
                                if int(group[2]) > 0:
                                    f.write(','.join(group) + '\n')
                                
                        send_events('traces/trace_' +aggregate+"_"+ str(myid) + '.csv', forwarding,myid,processing)             

            else: # process received primitive or complex event
               
                # cast strings to ints for ts and count
                myfields[1] = int(myfields[1]) 
                myfields[2] = int(myfields[2])
                
                #get aggregate for which event is input
                aggregate = get_aggregate(myfields[0],processing)    
                
                #pre-processing merge-step (sum aggregates)
                if not int(myfields[1]) in my_dicts[aggregate].keys():
                        my_dicts[aggregate][int(myfields[1])] = myfields
                else: 
                        my_dicts[aggregate][int(myfields[1])][2] += myfields[2] 
                        
                
            if sum(list(eof_counter.values())) >=  sum(list(predecessors.values())): # stop listening as soon as inputs for all aggregates received
                break
            
    finally:
        # close the socket
        print("Stopped listening.")
        sock.close()

       
        
def main():
    # start with node_id
    
    if len(sys.argv) < 2:
        myid = 1
    else:
        myid = int(sys.argv[1])
        
    csv_file_path = 'traces/trace_' + str(myid) +'.csv'
    json_file_path = 'plans/config_' + str(myid) +'.json'
 
    # read the JSON file and dynamically create variables
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # initialize fields from json config
    globals().update(data)
    global my_dicts
    for aggregate in processing.keys(): # instantiate dict for each aggregate for the respective events input to the aggregate
            my_dicts[aggregate] = {}
            
    # start listener, will be stopped immediately for nodes that are only sources 
    # listener should contain calls to summary
    listening_thread = threading.Thread(target=listen_to_port, args=(myid, processing,forwarding))
    listening_thread.start()
    
    # find out when to start sending events (used to synchronize operation of nodes)
    now = datetime.datetime.now()
    wait_seconds = 60 - now.second + (60 * (now.microsecond > 0))  
    #wait_seconds = 5
    print(f"Waiting for {wait_seconds} seconds to start at the next full minute...")
    time.sleep(wait_seconds)
    send_events(csv_file_path, forwarding,myid,processing)    
    
    
    listening_thread.join()          

          
if __name__ == "__main__":
    main()

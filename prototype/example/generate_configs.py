import json

### INPUTS
network = [[2335, 0, 547, 21364], [0, 90, 0, 0], [2335, 0, 547, 21364], [2335, 90, 547, 21364], [0, 90, 0, 0]]
#we push C's to all sources producings A's ; we transmit C's with aggregates for the projection AC to sources of E's
plan = [('C', 'A'), ('D', 'E'), ('AC', 'E', 'C')]
aggregate_placement = [('AC','A'), ('ACDE','E')]


def get_aggregate(eventtype):
    aggregates = [x[0] for x in aggregate_placement]
    my_aggregates = sorted([x for x in aggregates if eventtype in x and not x == eventtype],key = len)
    print("my_aggregates:",my_aggregates)
    if my_aggregates:
        return my_aggregates[0]
    else:
        return ''
    
def gettypes(network,query): # returns dict: keys are nodes, values are event types
    letters = query
    my_dict = {}   
    for pos in range(len(network)):
        my_dict[pos] = []
        node = network[pos]
        for letter in range(len(node)):
            if node[letter] > 0 :
                my_dict[pos].append(letters[letter])
    return my_dict

def getsources(network, query): #returns dict: keys are event types, values are nodes
    letters = query
    my_dict = {}
    for letter in range(len(network[0])):
        my_dict[letters[letter]] = []
    for pos in range(len(network)):
        node = network[pos]
        for letter in range(len(node)):
            if node[letter] > 0 :
                my_dict[letters[letter]].append(pos)
    return my_dict

def get_forwarding_entry(node, type_dict, source_dict, forwarding_dict, aggregates,input_dict):
    local_forwarding_entry = {}
    my_aggregates = [x for x in aggregates.keys() if node in aggregates[x]]
    send_rules = []
    for eventtype in type_dict[node]:
        if eventtype in forwarding_dict.keys():
            local_forwarding_entry[eventtype] = [source_dict[forwarding_dict[eventtype][0]],get_aggregate(eventtype)]
            send_rules.append(eventtype)
    for aggregate in my_aggregates:
        if aggregate in forwarding_dict.keys():
            local_forwarding_entry[aggregate] = [source_dict[forwarding_dict[aggregate][0]],get_aggregate(aggregate)]
            send_rules.append(aggregate)

    for aggregate in my_aggregates:
        for inputtype in input_dict[aggregate]:
            print(inputtype,send_rules,source_dict)
            if not inputtype in send_rules and node in source_dict[inputtype]:
                local_forwarding_entry[inputtype] = [[node],aggregate]
                send_rules.append(inputtype)

    return local_forwarding_entry       

def get_processing_entry(node,aggregate_dict,source_dict,forwarding_dict, type_dict):
    local_processing_entry = {}
    my_aggregates = []
    for aggregate in aggregate_placement:
        source_dict[aggregate[0]] = source_dict[aggregate[1]]
    for eventtype in type_dict[node]: 
        my_aggregates += [x[0] for x in aggregate_placement if x[1] == eventtype]

    for aggregate in my_aggregates:
        inputs = [x for x in list(set(list(source_dict.keys()) + list(forwarding_dict.keys()))) if aggregate==get_aggregate(x)]
        
        get_source = [x[1] for x in aggregate_placement if x[0] == aggregate]
        get_rate = [x[2] for x in plan if x[0] == aggregate]
        if not get_rate: #sink
            get_rate = get_source
        
        # only if input is actually sent    
        preds = [source_dict[x] for x in [y for y in inputs if y in forwarding_dict.keys()]]
        for i in inputs:
            if node in source_dict[i]:
                preds.append([node])
        predecessors = len(list(set(sum(preds,[])))) # number of distinct nodes computing inputs to aggregate
        
        local_processing_entry[aggregate] = [get_source[0], get_rate[0], inputs, predecessors]
    return local_processing_entry

def get_forwarding(plan): # write input to forwarding

    forwarding_dict = {}
    for my_tuple in plan:
        if len(my_tuple) == 2: # primitive event my_tuple[0] to be sent to my_tuple[1]
            for i in my_tuple[0]:
                if not i in forwarding_dict:
                    forwarding_dict[i] = [my_tuple[1]]
                else:
                    forwarding_dict[i].append(my_tuple[1])
        else: # aggregate my_tuple[0] to be sent to my_tuple[1] with rate my_tuple[2]
            if not my_tuple[0] in forwarding_dict: 
                forwarding_dict[my_tuple[0]] = [my_tuple[1]]
            else:
                forwarding_dict[my_tuple[0]].append(my_tuple[1])
    return forwarding_dict

def get_aggregate_dict(source_dict):
    aggregate_dict = {}
    for aggregate in aggregate_placement:
        aggregate_dict[aggregate[0]] = source_dict[aggregate[1]]
    return aggregate_dict
    

    
def main():
    
    query = ['A', 'C', 'D', 'E']
    source_dict = getsources(network, query)
    type_dict = gettypes(network, query)
    forwarding_dict = get_forwarding(plan)
    aggregate_dict = get_aggregate_dict(source_dict)
    
    
    input_dict = {}
    for aggregate in aggregate_placement:    
        input_dict[aggregate[0]] =  [x for x in list(set(list(source_dict.keys()) + list(forwarding_dict.keys()))) if aggregate[0]==get_aggregate(x)]
    
    for node in type_dict.keys():
       my_config = {"id": node, "forwarding":get_forwarding_entry(node, type_dict,source_dict, forwarding_dict,aggregate_dict,input_dict),"processing":   get_processing_entry(node,aggregate_dict,source_dict,forwarding_dict, type_dict)}
       print(my_config)
       with open('plans/config_' + str(node) +'.json', 'w') as f:
            json.dump(my_config, f)

if __name__ == "__main__":
    main()

from event_sourced_network import EventSourcedNetwork
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import zipf
import time

class PushPullAggregationPlanner:
    def __init__(self, sequence_pattern, number_of_nodes, zipfian_parameter, event_node_ratio):
        self.sequence_pattern = sequence_pattern
        self.number_of_nodes = number_of_nodes
        self.zipfian_parameter = zipfian_parameter
        self.event_node_ratio = event_node_ratio
        self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), number_of_nodes, zipfian_parameter, event_node_ratio, [])


    def predecessor_is_lower(self, sequence_pattern, idx):
        return self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx-1]] < self.event_sourced_network.eventtype_to_local_outputrate[sequence_pattern[idx]]

    def successor_is_lower(self, sequence_pattern, idx):
        return self.event_sourced_network.eventtype_to_global_outputrate[sequence_pattern[idx+1]] < self.event_sourced_network.eventtype_to_local_outputrate[sequence_pattern[idx]]

    @staticmethod
    def is_initiator_or_terminator_eventtype(sequence_pattern, eventtype):
        return sequence_pattern[0] == eventtype or sequence_pattern[-1] == eventtype


    def determine_cut_sequence_idx(self, lhs_strong_placement, rhs_strong_placement):
        lhs_strong_placement_idx = self.sequence_pattern.index(lhs_strong_placement)
        rhs_strong_placement_idx = self.sequence_pattern.index(rhs_strong_placement)
        
        minimal_rate = float('inf')
        minimal_type_idx = ''
        for idx in range(lhs_strong_placement_idx+1, rhs_strong_placement_idx):
            if self.event_sourced_network.eventtype_to_global_outputrate[self.sequence_pattern[idx]] < minimal_rate:
                minimal_rate = self.event_sourced_network.eventtype_to_global_outputrate[self.sequence_pattern[idx]]
                minimal_type_idx = idx

        return minimal_type_idx

    def cut_and_compose_PA_plan_generator(self):
        PA_plan = []
        strong_placements = []
        sequence_length = len(self.sequence_pattern)

        # determine strong placements
        for idx in range(sequence_length):
            if idx == 0 and self.successor_is_lower(self.sequence_pattern, idx):
                strong_placements.append(self.sequence_pattern[idx])
            elif idx == sequence_length - 1 and self.predecessor_is_lower(self.sequence_pattern, idx):
                strong_placements.append(self.sequence_pattern[idx])
            elif idx > 0 and idx < sequence_length - 1:
                if self.predecessor_is_lower(self.sequence_pattern, idx) and self.successor_is_lower(self.sequence_pattern, idx):
                    strong_placements.append(self.sequence_pattern[idx])

        if len(strong_placements) <= 1:
            push_plan = []
            for eventtype in self.event_sourced_network.eventtype_to_global_outputrate: 
                push_plan.append((eventtype, eventtype))
            
            return push_plan

        #we have more than 1 strong placement..
        # initiator position
        first_strong_placement = strong_placements[0]
        if self.sequence_pattern[0] not in strong_placements:
            pushed_types = []
            target_node = first_strong_placement

            step = (self.sequence_pattern[0:self.sequence_pattern.index(first_strong_placement)],target_node) 
            PA_plan.append(step)

        # terminator position
        last_strong_placement = strong_placements[-1]
        if self.sequence_pattern[-1] not in strong_placements:
            pushed_types = []
            target_node = last_strong_placement

            step = (self.sequence_pattern[self.sequence_pattern.index(last_strong_placement)+1:len(self.sequence_pattern)], target_node)
            PA_plan.append(step)

        
        cut_types = []
        
        for idx in range(len(strong_placements)-1):
            cut_idx = self.determine_cut_sequence_idx(strong_placements[idx], strong_placements[idx+1])
            cut_types.append(self.sequence_pattern[cut_idx])
            lhs_strong_placement_idx = self.sequence_pattern.index(strong_placements[idx])
            rhs_strong_placement_idx = self.sequence_pattern.index(strong_placements[idx+1])
            if not cut_idx-1 == lhs_strong_placement_idx:
                lhs_step = (self.sequence_pattern[lhs_strong_placement_idx+1:cut_idx],strong_placements[idx])
                PA_plan.append(lhs_step)
        
            rhs_step = (self.sequence_pattern[cut_idx:rhs_strong_placement_idx],strong_placements[idx+1])
            PA_plan.append(rhs_step)

        active_sources = {}
        for idx in range(len(PA_plan)):
            pushed_types = PA_plan[-1+idx][0]
            target_source = PA_plan[-1+idx][1]
            if target_source not in active_sources:
                active_sources[target_source] = []
                active_sources[target_source].append(target_source)
            for pushed_type in pushed_types:
                active_sources[target_source].append(pushed_type)
        
        idx = len(strong_placements)-1
        while idx > 0:
            node_idx = strong_placements[idx]
            projection = ''.join(sorted(active_sources[node_idx], key=self.sequence_pattern.index))
            rate = set(active_sources[node_idx]).intersection(set(cut_types)).pop()
            step = (projection, strong_placements[idx-1], rate, strong_placements[idx])
            PA_plan.append(step)
            idx -= 1

        return PA_plan

    def merge_PA_plan_steps(self, PA_plan):
        merged_push_steps = {}

        aggregate_steps = []

        for step in PA_plan:
            if len(step) == 2: 
                if step[1] not in merged_push_steps:
                    merged_push_steps[step[1]] = step[0]
                else:
                    merged_push_steps[step[1]] += step[0]
            else:
                aggregate_steps.append(step)

        merged_tuples = [(merged_push_steps[key], key) for key in merged_push_steps]
        return merged_tuples + aggregate_steps

    def determine_PA_plan_costs_esn_complete_topology(self, PA_plan):
        push_costs = 0
        aggregation_costs = 0
    
        for step in PA_plan:
            if len(step) == 2:
                for etype_to_send in step[0]:
                    target_node_etype = step[1]
                    num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(etype_to_send, target_node_etype)
                    
                    push_costs += self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send] * len(self.event_sourced_network.eventtype_to_nodes[target_node_etype]) - num_nodes_producing_both_etypes * self.event_sourced_network.eventtype_to_local_outputrate[etype_to_send]
            elif len(step) > 2:
                for etype_to_send in step[2]:
                    target_node_etype = step[1]
                    multi_sink_placement_partitioning_etype = step[3]
                    num_nodes_producing_both_etypes = self.event_sourced_network.nodes_producing_both_eventtypes(multi_sink_placement_partitioning_etype, target_node_etype)
                    
                    num_multi_sink_placement_nodes = len(self.event_sourced_network.eventtype_to_nodes[multi_sink_placement_partitioning_etype])
                    target_nodes = len(self.event_sourced_network.eventtype_to_nodes[target_node_etype])

                    aggregation_costs += self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send] * num_multi_sink_placement_nodes *  target_nodes  - num_nodes_producing_both_etypes * self.event_sourced_network.eventtype_to_global_outputrate[etype_to_send]
        
        return push_costs + aggregation_costs

    def determine_AND_query_costs(self):
        lowest_global_output_rate_eventtype = min(self.event_sourced_network.eventtype_to_global_outputrate,key=self.event_sourced_network.eventtype_to_global_outputrate.get)

        missing_event_types = [char for char in self.sequence_pattern if char != lowest_global_output_rate_eventtype]

        PA_plan = []
        push_step = (lowest_global_output_rate_eventtype, missing_event_types[0])
        PA_plan.append(push_step)
        for idx in range(1,len(missing_event_types)):
            aggregation_step = (lowest_global_output_rate_eventtype, missing_event_types[idx], lowest_global_output_rate_eventtype, missing_event_types[idx-1])
            PA_plan.append(aggregation_step)

        return self.determine_PA_plan_costs_esn_complete_topology(PA_plan)

    
    def determine_centralized_push_costs(self):
        sink_costs = sum(max(self.event_sourced_network.nodes, key=sum))
        total_costs = sum(self.event_sourced_network.eventtype_to_global_outputrate.values())

        return total_costs - sink_costs

    def run_network_sizes_experiment(self):
        network_sizes = [5, 10, 20, 50, 100, 200, 500]
        transmission_ratios_per_network_size = {size: [] for size in network_sizes}
        first_rates = []
        for _ in range(10000):
            self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), 5, self.zipfian_parameter, self.event_node_ratio, [])
            first_rates = self.event_sourced_network.local_output_rates
            for size in network_sizes:
                self.number_of_nodes = size

                self.event_sourced_network = EventSourcedNetwork(len(self.sequence_pattern), size, self.zipfian_parameter, self.event_node_ratio, first_rates)
                PA_plan = self.cut_and_compose_PA_plan_generator()
                PA_plan = self.merge_PA_plan_steps(PA_plan)
                PA_plan_costs = self.determine_PA_plan_costs_esn_complete_topology(PA_plan)
                #PA_plan_costs = self.determine_AND_query_costs()
                centralized_push_costs = self.determine_centralized_push_costs()
                transmission_ratio = PA_plan_costs / centralized_push_costs

                transmission_ratios_per_network_size[size].append(min(transmission_ratio, 1.0)) 
    
        _, ax = plt.subplots()
        positions = np.arange(len(network_sizes))
        
        ax.boxplot(transmission_ratios_per_network_size.values(), positions=positions, patch_artist=True)

        ax.set_ylabel('Transmission Ratio', fontsize=20)
        ax.set_yscale('log')
        ax.set_xticks(positions)
        ax.set_xticklabels(network_sizes)
        ax.tick_params(axis='y', which='major', labelsize=20)
        ax.tick_params(axis='x', which='major', labelsize=15)
        ax.set_xlabel('Number of Nodes', fontsize=20)
        ax.set_title('Increasing Network Sizes for Sequence Pattern', fontsize=17, y=1.05)
        plt.tight_layout()
        plt.grid()
        plt.savefig('transmission_ratios_per_network_size_SEQ.pdf', format='pdf', dpi=300, bbox_inches='tight')
        plt.show()


def main():
    sequence_length = 20
    sequence = ''
    for idx in range(sequence_length):
        sequence += chr(65+idx)
    number_of_nodes = 20
    zipfian_parameter = 1.1
    event_node_ratio = 0.5
    
    PA_plan_generator = PushPullAggregationPlanner(sequence, number_of_nodes, zipfian_parameter, event_node_ratio)
    PA_plan_generator.run_network_sizes_experiment()


if __name__ == "__main__":
    main()
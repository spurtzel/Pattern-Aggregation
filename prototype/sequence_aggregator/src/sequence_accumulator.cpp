#include "event.hpp"

#include <boost/multiprecision/cpp_int.hpp>

#include <cxxopts.hpp>

#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>
#include <string>
#include <vector>

using counter_type = boost::multiprecision::uint256_t;
using timestamp_type = std::size_t;

template <> struct fmt::formatter<counter_type>: fmt::ostream_formatter {};

struct accumulated_event
{	
	suse::event<timestamp_type> event;
	counter_type count;
	std::string accumulation;

	friend std::istream& operator>>(std::istream& in, accumulated_event& e)
	{
		return in>>e.event>>e.count>>e.accumulation;
	}
};

int main(int argc, char* argv[]) try
{
	cxxopts::Options options("sequence_accumulator", "Computes accumulated matches of a sequence query");
	options.add_options()
		("sequence,s","Sequence to evaluate",cxxopts::value<std::string>())
		("type,t","The event type you are interested in", cxxopts::value<char>())
		("help,h","Display this help meassage");

	options.parse_positional("sequence");
	options.positional_help("sequence");

	const auto parsed_args = options.parse(argc,argv);

	if(parsed_args.count("help")>0 || argc<2)
	{
		fmt::print("{}",options.help());
		return 0;
	}

	for(auto required: {"sequence","type"})
	{
		if(parsed_args.count(required)==0)
		{
			fmt::print(stderr,"{} is a required argument\n",required);
			return 1;
		}
	}

	const auto relevant_type = parsed_args["type"].template as<char>();
	const auto seq = parsed_args["sequence"].template as<std::string>();
	const auto event_to_position = [&]()
	{
		std::array<std::size_t,256> result{};
		for(std::size_t i=0;i<seq.size();++i)
			result[seq[i]] = i;

		return result;
	}();

	const auto empty_counters = std::vector<counter_type>(seq.size()+1,0);

	auto active_counters_per_length = empty_counters;
	active_counters_per_length[0] = 1;

	struct counted_event_data
	{
		timestamp_type timestamp;
		std::vector<counter_type> counters_per_length;
	};
	std::vector<counted_event_data> per_event_counters;
	
	for(accumulated_event next_event; std::cin>>next_event;)
	{
		const auto append_after = event_to_position[next_event.accumulation.front()];
		const auto append_at = append_after+next_event.accumulation.size();
		active_counters_per_length[append_at]+=active_counters_per_length[append_after]*next_event.count;
		for(auto& e: per_event_counters)
			e.counters_per_length[append_at]+=e.counters_per_length[append_after]*next_event.count;

		if(next_event.event.type==relevant_type)
		{
			per_event_counters.push_back({ .timestamp = next_event.event.timestamp, .counters_per_length = empty_counters});
			per_event_counters.back().counters_per_length[append_at]+=active_counters_per_length[append_after]*next_event.count;
		}
	}

	for(const auto& e: per_event_counters)
		fmt::print("{} {} {} {} ",relevant_type,e.timestamp,e.counters_per_length.back(),seq);

	return 0;
}
catch(const cxxopts::exceptions::exception& e)
{
	fmt::print(stderr,"Error parsing arguments: {}\n", e.what());
	return 1;
}

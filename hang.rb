require 'arrow'

100.times do |index|
  puts index
  plan = Arrow::ExecutePlan.new
  left_table = Arrow::Table.load('foo.arrow')
  node = plan.build_source_node(left_table)
  %w[bar baz qux].each do |right_name|
    right_table = Arrow::Table.load("#{right_name}.arrow")
    right_node = plan.build_source_node(right_table)
    join_options = Arrow::HashJoinNodeOptions.new(:left_outer, [right_name], [right_name])
    node = plan.build_hash_join_node(node, right_node, join_options)
  end
  sink_node_options = Arrow::SinkNodeOptions.new
  plan.build_sink_node(node, sink_node_options)
  plan.validate
  plan.start
  plan.wait
end

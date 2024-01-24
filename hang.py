import pyarrow as pa
import pyarrow.acero as acero

for index in range(100):
    print(index)
    left_table = pa.table(pa.ipc.open_file("foo.arrow").read_all())
    node = acero.Declaration("table_source", acero.TableSourceNodeOptions(left_table))
    for right_name in ["bar", "baz", "qux"]:
        right_table = pa.table(pa.ipc.open_file(f"{right_name}.arrow").read_all())
        right_node = acero.Declaration(
            "table_source", acero.TableSourceNodeOptions(right_table)
        )
        join_options = acero.HashJoinNodeOptions(
            join_type="left outer", left_keys=[right_name], right_keys=[right_name]
        )
        node = acero.Declaration("hashjoin", join_options, [node, right_node])
    node.to_table()

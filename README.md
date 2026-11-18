# 靶点证据湖分支验证与主线提升

此工程提供研究原始资料的物理检查和内容寻址对象工具。资料全部合成，不对应真实个人、研究结论或医疗建议。逻辑湖把实验证据保存为两张真实 Parquet 表（`observations`、`target_map`），两者共同构成一个验证对象；分支从明确的主线快照建立，可独立修订、验证，并以保留主线并发写入的方式提升。

Python 3.11，依赖 DuckDB 1.4.1、PyArrow 22.0.0。安装使用 `python -m pip install -r requirements.txt`。可以执行 `docker build -t research-lake .` 后运行 `docker run --rm research-lake`，镜像默认检查原始资料清单，不访问外部业务服务。

`python -m lakekit inventory data/inventory.json` 校验实际文件摘要、大小和 Parquet 行数。`python -m lakekit --root .local/lake ...` 提供逻辑湖命令（见下）。`python -m unittest discover -s tests -v` 检查对象损坏、路径约束、独立 JSON 与 Parquet 的值一致性，以及分支修订、三路合并、绑定验证、竞争提升和 DuckDB 快照查询。`python -m compileall -q lakekit tests` 检查源码语法。入口可从工程根目录运行。

`data/seed.json` 与同名 Parquet 表保存同一份原始值，`data/events.json` 保存尚未应用的业务动作，`data/contracts.json` 记录输入字段和标识约定。事件不是已有功能的执行结果，只有显式运行 `apply-events` 或 `revise` 才会写入湖。示例事件本身会制造一次真实的拒绝：`review` 删除 `T1:v1` 后，主线新增的 `O2` 仍引用它；`O3` 引用的 `T2:v1` 从未建立。`validate` 会报告 `dangling_reference`，`promote` 返回失败并保留分支与详情。

## 逻辑表与映射键

| 表 | 主键 | 列 |
| --- | --- | --- |
| `observations` | `observation_id` | `observation_id`、`map_key`、`score`（可空字符串） |
| `target_map` | `map_key` | `map_key`、`canonical_target`（可空字符串） |

映射键恒为 `source_target:mapping_version`，两个分量非空且不含冒号。每条观察显式绑定完整的映射键；`canonical_target` 只是映射内容，相同显示名不构成同一个键，也不参与匹配。

## 库接口

```python
from lakekit.lake import Lake, insert, update, delete

Lake.initialize(root, observations, target_map)   # 建立主线 v1；拒绝覆盖已有目录
lake = Lake(root)

lake.create_branch("review", snapshot=None)       # 固定一条明确的主线快照
lake.revise("review", [update("observations", "O1", {"score": "0.9"}),
                       delete("target_map", "T2:v1"),
                       insert("observations", {"observation_id": "O3",
                                               "map_key": "T2:v1", "score": "0.2"})])
lake.preview("review", operations)                # 试算，不写入
lake.apply_events(events)                         # 显式应用事件文件的内容

lake.diff("review", against="base")               # 分支真实行级差异 + 当前冲突
lake.status("review")                             # 主线/分支/冲突/记录
lake.validate("review", snapshot=None)            # 绑定分支快照与当时主线快照
lake.revalidate(validation_id)                    # 主线或分支变化后重跑全部检查
lake.promote(validation_id)                       # 三路合并并原子替换主线指针

lake.show("review", table=None)                   # 固定快照的真实行
lake.query("SELECT ...", "review", snapshot=None) # DuckDB 查询固定快照
lake.snapshot_manifest("main")                    # 快照摘要与行数
lake.history("review")                            # 修订、验证与提升记录
```

每次修订、验证、提升都返回可直接 JSON 序列化的字典；行内容、差异和冲突详情都来自目录中实际保存的 Parquet 表，而不是调用参数或上传记录。

## 合并与验证语义

- 分支固定的是主线快照；之后主线继续接收的新资料不会进入旧分支的查询。
- 修订以显式主键为单位：同一 `observation_id` 在两边得到不同内容、一边删除另一边修改、两边对同一 `map_key` 作不同变更，都报告冲突；两边最终内容完全相同可以合并。
- 主线自分叉后的无冲突写入在提升时保留。主键合并没有逐键冲突并不等于安全：合并后的两张表还要整体复核，任何观察都必须能追到唯一映射。主线新增观察仍引用分支删掉的映射时，会报告 `dangling_reference`。
- 验证记录同时绑定分支快照、分支修订序号、主线快照和主线版本。验证通过时会保存合并结果的摘要；提升时在同一把锁里重读目录、重新三路合并并复核引用，只有合并内容与验证记录一致才替换主线指针。主线在验证后变化时返回 `stale_validation`，分支在验证后变化时返回 `branch_changed`。
- 提升失败返回 `promoted: false` 及原因、逐键冲突和引用冲突详情，并追加到提升尝试记录；分支、主线都不变。研究人员可继续修订分支，重新验证后再申请提升。
- 竞争提升在进程间互斥锁内串行；失败者得到确定的 `stale_validation`，可以重新检查或重新验证，不会出现“文件已上传但主线未变却报告成功”。提升成功前会重新读回目录确认指针。

## 命令行入口

```sh
python -m lakekit --root .local/lake init --seed data/seed.json
python -m lakekit --root .local/lake branch create review
python -m lakekit --root .local/lake apply-events data/events.json
python -m lakekit --root .local/lake revise review \
  --insert target_map '{"map_key":"T2:v1","canonical_target":"G2"}' \
  --insert observations '{"observation_id":"O3","map_key":"T2:v1","score":"0.2"}'
python -m lakekit --root .local/lake status review
python -m lakekit --root .local/lake diff review
python -m lakekit --root .local/lake validate review
python -m lakekit --root .local/lake promote V000004-xxxxxxxx
python -m lakekit --root .local/lake show main
python -m lakekit --root .local/lake query review --sql "SELECT * FROM observations"
python -m lakekit --root .local/lake history review
```

`preview --operations ops.json` 可试算一组修订；`query` 支持 `--snapshot` 固定历史快照和 `--param` 位置参数。`python -m lakekit data/inventory.json` 保留原有物理清单检查（也可写作 `python -m lakekit inventory data/inventory.json`）。

## 存储

`<root>/objects/<aa>/<62 位摘要>` 保存内容寻址的 Parquet 表和快照清单，`<root>/catalog.json` 保存主线、分支、验证与提升记录，`<root>/catalog.lock` 用于进程间互斥。修订总是写入新对象再原子替换目录指针；旧快照与失败记录不会被覆盖。

`lakekit.objects.Objects` 仍提供对象按摘要写入、读取校验及完整本地目录枚举；`lakekit.physical` 仅打开明确指定的单个 Parquet，`lakekit.inventory` 核对种子清单。逻辑湖复用这些工具，但不预设外部服务或发布流程。


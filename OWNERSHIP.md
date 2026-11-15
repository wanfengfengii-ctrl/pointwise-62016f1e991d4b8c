# 重构后职责与状态归属

执行链只有一条：`spill.aggregate` → `pipeline.aggregate` → `Rules` + `Driver` + `order`。
`spill.py` 仅做导入转发，无旧闭包、无复制执行器、无条件回退。

| 模块 | 职责 | 持有状态 |
| --- | --- | --- |
| reduction.py `Rules` | `lift` 行校验成段单元、`key` 可排序键、`combine` 同键合并、`finish` 流式归约；只读入参并返回新值或惰性结果 | 无。无文件/句柄/工作区/回调/可变全局 |
| sorter.py `order` | 限额切块、排序、分轮分批、堆归并与拉取顺序；经 `rules.lift/key/combine` 处理单元 | 仅段标识、令牌、单元与排序键；**不持物理句柄** |
| driver.py `Driver` | 独占真实 Handle、活动句柄登记、待删段所有权登记；单次段操作与清理 | 句柄登记表与段所有权表；令牌是不透明可散列值 |
| pipeline.py | 参数校验、输入迭代器与最终输出生命周期、错误优先级；经 `Rules.finish` 消费 `order` | 只持 `source`/`ordered`/`output` 三个生命周期对象 |
| cleanup.py | 通用清理工具：逐项执行、收集、按优先级抛出 | 无 |

边界：`order` 不调用 `finish`、不接触 `Workspace`/`Handle`/目录，不自持输入 close，正常结束也不调用 `driver.cleanup`；`Driver` 不比较键、不合并、不遍历单元流；pipeline 不解释业务字段、不含排序/归并循环。`workspace.py` 未改动。

## 最后一组交付后的资源交接

1. 最后一次 `next` 只消费到最后一组的归约单元即返回，不提前结束。
2. 随后的 `next`（或显式 `close` 的 GeneratorExit 路径）触发 pipeline 的 finally：
   关闭输入 → 结束最终归并（`order` 的 `_merge` 关闭其读令牌）→ 关闭 `finish` 输出迭代器 → `driver.cleanup()` 逆序关闭遗留句柄、逆序删除仍归本次作业所有的段。
3. 单个清理动作失败也继续其余动作；已有主异常时不被清理异常替代，正常结束时抛出清理中首个异常。
4. `Driver.close/remove` 即使事件注入失败也先完成所有权登记释放，且不重试，故 `live`/`remaining` 在结束、失败或 close 后均为空。

## 核验

`python3 verify_all.py`（即 `.pointwise-runtime.json` 的 verify）= 原 `checks.py verify` + 真实磁盘 `checks.py disk` + `structure_checks.py` 结构验证（Rules 纯度与流式、opaque 单元下的 order 单遍/限额/扇入、Driver 所有权释放与逆序错误收集、导入与源码边界）。`demo` 仍为真实文件汇总结果与完整轨迹。

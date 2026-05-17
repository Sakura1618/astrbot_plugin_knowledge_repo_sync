# knowledge_repo_sync

[![AstrBot](https://img.shields.io/badge/AstrBot-Plugin-blue)](https://github.com/Soulter/AstrBot) [![Python](https://img.shields.io/badge/Python-3.10+-green)](https://www.python.org/)

从远程 Git 仓库同步 Markdown 文档到 AstrBot 知识库，支持自动建库、分支识别、Git 差异检测增量同步、忽略路径、通知和分块参数配置。

## 功能

- 支持从远程 Git 仓库同步 Markdown 文档到 AstrBot 知识库
- 支持自动识别仓库默认分支，也支持手动指定分支
- 支持自动创建目标知识库
- 支持基于 Git 提交差异的增量同步，避免每次全量重建
- 支持忽略指定文件或目录
- 支持管理员手动触发同步
- 支持定时自动检测远端变化并同步
- 支持给主人或指定群发送同步通知
- 支持配置 chunk size、chunk overlap 和嵌入批处理参数

## 同步逻辑

插件会记录上一次成功同步时的远程仓库地址、分支和提交 SHA。

正常情况下：

- 如果远端 HEAD 没变，不会执行同步
- 如果远端 HEAD 变了，会对比上次同步提交和当前提交之间的 Git diff
- 只处理发生变化的 Markdown 文件
  - 新增文件：导入知识库
  - 修改文件：删除旧文档后重新导入
  - 删除文件：从知识库删除对应文档

以下情况会自动退回全量同步：

- 首次同步
- 目标知识库不存在，需要新建
- 仓库地址变更
- 分支变更
- 插件没有找到上次同步记录
- 当前运行环境无法对比 `last_sync_head..remote_head`
- 分块配置发生变化，需要按新参数重新入库

## 安装

推荐使用 AstrBot 插件市场进行安装。

### 手动安装：
将插件放到：

```text
data/plugins/knowledge_repo_sync
```

当前目录结构示例：

```text
knowledge_repo_sync/
  README.md
  main.py
  metadata.yaml
  _conf_schema.json
```

然后在 AstrBot 插件管理中启用插件，并填写配置。

## 配置项

推荐使用 AstrBot 网页端进行配置。

### 基础配置

- `target_kb_name`
  - 目标知识库名称
  - 优先同步这个知识库
  - 如果该知识库不存在，插件会尝试自动创建

- `new_kb_name`
  - 当目标知识库不存在时，用于自动创建的新知识库名称
  - 如果已经填写 `target_kb_name`，通常可以不填

- `remote_repo_url`
  - 远程仓库完整地址
  - 例如：`https://github.com/example/knowledge-repo`

- `remote_branch`
  - 远程仓库分支
  - 留空时自动使用远程仓库默认分支

- `ignore_paths`
  - 需要跳过的仓库相对路径
  - 支持文件或目录
  - 例如：`README.md`、`docs/tmp`

### 文档切分配置

- `chunk_size`
  - 文本分块大小
  - 默认 `512`

- `chunk_overlap`
  - 文本分块重叠大小
  - 默认 `50`

如果这两个配置发生变化，插件会回退到全量同步，保证知识库中的全部文档按新参数重新切分。

### 嵌入配置

- `embedding_batch_size`
  - 单次嵌入批处理大小
  - 默认 `64`

- `embedding_tasks_limit`
  - 嵌入并发任务数
  - 默认 `1`

- `embedding_max_retries`
  - 嵌入失败最大重试次数
  - 默认 `7`

### 通知配置

- `notify_owner_enabled`
  - 是否给主人发送同步通知

- `notify_group_enabled`
  - 是否向指定群发送同步通知

- `notify_group_id`
  - 接收通知的群号

### 自动同步配置

- `auto_sync_enabled`
  - 是否启用自动检测同步

- `auto_sync_interval_hours`
  - 自动检测间隔，单位小时
  - 默认 `24`

## 使用方式

### 手动同步命令

- `/knowledge_sync`

该命令需要管理员权限。

执行后，插件会：

- 检查当前是否已有同步任务在运行
- 拉取远程仓库当前分支快照
- 判断是执行增量同步还是全量同步
- 输出同步结果，包括：
  - 当前 Markdown 总数
  - 新增/更新文件数量
  - 删除文件数量

### 自动同步

开启 `auto_sync_enabled` 后，插件会按 `auto_sync_interval_hours` 定时检查远端仓库 HEAD 是否变化。

只有检测到远端变化时才会执行同步。

## 文档匹配规则

插件使用知识库文档名 `doc_name` 与仓库中的相对路径进行匹配。

例如仓库中存在：

```text
docs/intro.md
guides/install.md
```

则知识库内对应文档名也会是：

- `docs/intro.md`
- `guides/install.md`

这意味着：

- 同一路径文件内容变更时，会覆盖更新该文档
- 文件被删除时，会删除该路径对应的知识库文档
- 文件改名时，效果等同于“旧路径删除 + 新路径新增”

## 注意事项

- 当前仅同步 Markdown 文件，支持的后缀为：`.md`、`.markdown`
- `.git` 目录内的内容不会被导入
- `ignore_paths` 中配置的文件或目录不会被同步
- 插件需要运行环境可以访问远程 Git 仓库
- 插件依赖系统可用的 `git` 命令
- 自动创建知识库时，会默认选择当前 AstrBot 中第一个可用的嵌入模型
- 如果嵌入模型不可用，自动建库会失败
- 删除知识库文档时，插件也会尝试清理对应的媒体记录和落盘媒体文件

## 常见问题

### 1. 为什么这次没有走增量同步？

可能原因：

- 这是首次同步
- 修改了 `chunk_size` 或 `chunk_overlap`
- 切换了仓库地址或分支
- 上次同步记录丢失
- Git diff 无法成功计算

### 2. 为什么远端变了但没有同步？

可能原因：

- 变化不在 Markdown 文件内
- 变化的路径被 `ignore_paths` 忽略了
- 自动同步尚未到达下一次检测时间

### 3. README 变更会不会同步？

默认不会，因为默认忽略路径里包含 `README.md`。

如果你希望同步仓库中的 README，需要把它从 `ignore_paths` 中移除。

## 适用场景

- 用 Git 仓库维护项目文档、FAQ、知识手册
- 让 AstrBot 知识库跟随文档仓库持续更新
- 文档量较大，不希望每次变更都全量重建知识库

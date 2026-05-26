# pixiv-agent

Pixiv 数据代理节点，负责拉取并本地缓存 Pixiv 作品元数据和图片文件，供主服务器（Moetopia backend）轮询导入。

## 架构角色

```
Moetopia backend  ──(HTTP, X-API-Key)──▶  pixiv-agent
                                               │
                                         PixivAPI + 本地 SQLite + 图片缓存
```

主服务器**不直接访问 Pixiv**，所有 Pixiv 数据流经节点代理，节点通过速率限制队列避免被识别为恶意爬虫。

## 快速启动

```bash
cp .env.example .env
# 编辑 .env，填写 PIXIV_REFRESH_TOKEN 和 AGENT_API_KEY

pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8100
```

## API 鉴权

所有接口需要 `X-API-Key: <your_api_key>` 请求头。

## 主要接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 节点状态 + 队列深度 |
| POST | `/sync/author/{pixiv_user_id}` | 加入同步队列 |
| GET  | `/sync/status` | 队列状态 |
| GET  | `/sync/authors` | 已追踪作者列表 |
| GET  | `/artworks` | 缓存作品列表（支持 `?since=ISO时间&pixiv_user_id=xxx`） |
| GET  | `/artworks/{pixiv_id}` | 单作品详情（含图片列表） |
| GET  | `/artworks/{pixiv_id}/images/{index}` | 流式返回图片文件 |

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PIXIV_REFRESH_TOKEN` | Pixiv Refresh Token | （必填） |
| `AGENT_API_KEY` | 节点鉴权密钥 | `changeme` |
| `PORT` | 监听端口 | `8100` |
| `RATE_LIMIT` | Pixiv API 速率 (req/s) | `0.5` |
| `DOWNLOAD_CONCURRENCY` | 图片并发下载数 | `2` |
| `MAX_ARTWORKS_PER_AUTHOR` | 每作者最大作品数（0=无限） | `0` |
| `NODE_NAME` | 节点名称 | `agent-01` |
| `DATA_DIR` | SQLite + 图片缓存目录 | `./data` |

## Docker

```bash
docker build -t pixiv-agent .
docker run -d \
  -p 8100:8100 \
  -v $(pwd)/data:/app/data \
  --env-file .env \
  pixiv-agent
```

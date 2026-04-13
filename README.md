# LocalMovieBackup

一个使用 Go 编写的备份服务，用于接收客户端上传的 `sqlite` + `json` 文件，压缩后存储到阿里云 OSS，并支持列表查询与下载。

## 功能

1. 上传备份：
   - 客户端上传：
     - 备份名称（`name`）
     - sqlite 文件（字段：`sqlite`）
     - json 文件（字段：`json`）
   - 服务端会：
     - 校验参数
     - 将两个文件和 `manifest.json` 打成 zip
     - 上传到 OSS
2. 查询备份列表：返回备份名称、创建时间、ID、大小等
3. 下载备份：根据备份 ID 下载 zip 文件
4. 不使用数据库，只依赖 OSS 对象和元数据

## 环境变量

复制 `.env.example` 并配置：

- `PORT`：服务端口（默认 `8080`）
- `OSS_ENDPOINT`：OSS Endpoint
- `OSS_BUCKET`：OSS Bucket 名
- `OSS_ACCESS_KEY_ID`：AccessKey ID
- `OSS_ACCESS_KEY_SECRET`：AccessKey Secret
- `OSS_PREFIX`：对象前缀（默认 `backups/`）
- `MAX_UPLOAD_MB`：上传体积上限（MB，默认 `100`）

## 启动

```bash
go mod tidy
go run .
```

## API

### 1) 上传备份

`POST /api/backups`

`multipart/form-data` 字段：
- `name`：备份名称
- `sqlite`：sqlite 文件
- `json`：json 文件

示例：

```bash
curl -X POST http://localhost:8080/api/backups \
  -F "name=my-backup" \
  -F "sqlite=@./app.db" \
  -F "json=@./meta.json"
```

### 2) 列表

`GET /api/backups`

```bash
curl http://localhost:8080/api/backups
```

### 3) 下载

`GET /api/backups/{id}`

```bash
curl -L "http://localhost:8080/api/backups/20260413T120000Z" -o backup.zip
```

> 说明：`id` 优先读取 OSS 对象元数据 `backup-id`，如果缺失则回退到对象文件名。

# geofill-address-server

给 [GeoFill](https://github.com/debbide/GeoFill) 浏览器扩展用的自托管地址 API。
轻量、开箱即用：数据随镜像发布，`docker compose up -d` 后立刻可用，
不下载、不同步、不需要第三方 API key。

## 特性

- **开箱即用**：地址数据（来自 Overture Maps 开放数据）预先抽样打进镜像，
  启动即用，没有漫长的 ETL/同步等待
- **真实可查**：每条街道都是真实存在的街道 + 真实城市邮编，
  拼出的地址可在 Google 地图定位
- **零依赖**：服务端仅用 Python 标准库（`http.server` + `sqlite3`）
- **与 GeoFill 原生对接**：实现 GeoFill 自托管地址源协议，插件零改动

## 快速开始

```bash
mkdir -p ~/geofill-address && cd ~/geofill-address
curl -fsSLo docker-compose.yml https://raw.githubusercontent.com/debbide/geofill-address-server/main/docker-compose.yml
echo "API_BIND_ADDRESS=0.0.0.0" > .env   # 允许局域网访问（默认只绑 127.0.0.1）
docker compose up -d
curl "http://127.0.0.1:8788/api/v1/generate?country=US"
curl http://127.0.0.1:8788/health
```

然后在 GeoFill 扩展设置中填入 `http://<本机局域网IP>:8788`，
作为自托管地址服务地址即可。

## API

### `GET /api/v1/generate`

| 参数 | 说明 |
|---|---|
| `country` | ISO 国家代码（US/GB/JP/...），必填 |
| `city` | 可选；该城市有数据时优先返回该城市，否则全国随机 |

响应：

```json
{
  "data": {
    "result": {
      "address": {
        "components": {
          "houseNumber": "350",
          "street": "5th Ave",
          "locality": "New York",
          "admin1": "New York",
          "postcode": "10118"
        },
        "formattedAddress": "350 5th Ave, New York, NY 10118",
        "matchLevel": "premise"
      }
    }
  }
}
```

无数据时返回 `404 {"error": "NO_POOL_COVERAGE"}`，调用方应降级处理
（GeoFill 会自动回退到本地地址池）。

### `GET /health`

返回 `{"status": "ok"}`。

### 认证（可选）

设置环境变量 `API_TOKEN` 后，所有 `/api/v1/generate` 请求需带
`Authorization: Bearer <token>`。GeoFill 扩展的设置中可填写 token。

## 数据

- 来源：[Overture Maps](https://overturemaps.org/) addresses 主题
  （开放数据，需署名；见 `data/` 及库内 `meta` 表）
- 覆盖：12 国 × 每国 Top 10 城市 × 每城 20 条街道，共 2159 条
  （US CA MX DE FR IT ES NL HK SG AU BR）
- IT/DE/FR 部分街道的邮编由 GeoNames 城市邮编表补齐；HK 无邮编（当地惯例）
- Overture 无数据的国家（GB RU CN KR IN）及未纳入的 JP/TW，
  由 GeoFill 本地地址池兜底
- 更新：重新运行 `etl/extract3.py` 后 `--finalize` 重建 `data/addresses.db`
  并重新构建镜像

本地构建数据库：

```bash
pip install boto3 pyarrow
python3 etl/extract3.py           # 断点续跑，状态在 etl/checkpoints/
python3 etl/topupsample.py        # 补扫主扫中缺失的 Top 城市（计数精确后定向补）
python3 etl/extract3.py --finalize # 产出 data/addresses.json + data/addresses.db
```

注意：ETL 需要能直连 Overture Maps 的 S3（或可用的 HTTP 代理）。

## 许可

代码 MIT。地址数据遵循 Overture Maps 的开放数据许可（署名 Overture Maps contributors）。

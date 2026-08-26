# Binance USDT無期限先物 PUMP候補 Discord通知Bot

## フィルター条件

対象はBinance USDⓈ-M Futures の USDT建て無期限先物です。

ローソク足：1時間足、確定足のみ
実装されている条件

通知されるのは、最新の確定済み1時間足について、

陽線
AND
出来高 >= 10M USDT
AND
出来高 >= 直前24本の確定足平均 × 2
AND
始値→終値の上昇率 >= +10%
AND
最新終値が直近24本の確定足終値で最高

をすべて満たした場合だけです。

## ファイル構成

```text
main.py
requirements.txt
.env.example
.github/workflows/deploy.yml
systemd/binance-pump-alert.service
```

## VPS初回セットアップ例

Ubuntu想定です。

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip
sudo mkdir -p /opt
sudo chown -R $USER:$USER /opt
cd /opt
git clone <あなたのGitHubリポジトリURL> binance_pump_alert_bot
cd /opt/binance_pump_alert_bot
cp .env.example .env
nano .env
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`.env` の `DISCORD_WEBHOOK_URL` にDiscord Webhook URLを入れてください。

## systemd登録

`systemd/binance-pump-alert.service` 内の `User` と `WorkingDirectory` が実環境と合っているか確認してください。

```bash
sudo cp systemd/binance-pump-alert.service /etc/systemd/system/binance-pump-alert.service
sudo systemctl daemon-reload
sudo systemctl enable binance-pump-alert.service
sudo systemctl start binance-pump-alert.service
sudo systemctl status binance-pump-alert.service
```

ログ確認:

```bash
journalctl -u binance-pump-alert.service -f
```

## GitHub Secrets

GitHubリポジトリの `Settings > Secrets and variables > Actions` に以下を登録してください。

- `VPS_HOST`: VPSのIPアドレスまたはホスト名
- `VPS_USER`: SSHユーザー名 例: `ubuntu`
- `VPS_SSH_KEY`: VPSへSSH接続できる秘密鍵
- `VPS_PORT`: 通常は `22`
- `VPS_APP_DIR`: 例: `/opt/binance_pump_alert_bot`

以後、`main` ブランチへpushするとGitHub ActionsがVPSへSSH接続し、`git pull`、依存関係更新、Bot再起動を行います。

## 注意

- Discord Webhook URLはGitHubに直接コミットしないでください。
- Binance APIの制限に配慮し、`REQUEST_CONCURRENCY` は最初は `8` 程度を推奨します。
- 通知Botであり、自動売買は行いません。

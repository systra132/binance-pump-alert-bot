# Binance USDT無期限先物 PUMP候補 Discord通知Bot

## フィルター条件

項目	条件
対象銘柄	Binanceの全USDT無期限先物
使用足	確定1時間足のみ
MA	MA5・MA10がそれぞれMA30・MA50・MA100より上
高値圏	最新確定足の高値が、最新足を含む直近24本の1時間足高値の上位3位以内
ローソク足	最新確定足が陰線
上ヒゲ	上ヒゲの長さが実体の100%以上
資金調達率	プラス
出来高	最新確定足が1M USDT以上
相対出来高	最新確定足が直前24本の平均出来高の1.5倍以上
OI	最新確定1時間足のOIが、過去24時間のOI最低値より5%以上増加

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

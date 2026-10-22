# DeepSeek 用量帳本

> DeepSeek 餘額每小時記帳：每天真實花費、聊天/後臺/畫圖分項、餘額不足私信主人

AstrBot 插件，屬於「擬真群友」（lively）插件套件。適用 AstrBot >=4.29.0 + NapCat（aiocqhttp）。

主人、群、機器人名字、人設用詞這些共用設定在 [astrbot_plugin_lively_gate](https://github.com/GUCine/astrbot_plugin_lively_gate) 裡填，建議一起裝。

## 功能

```text
機器人的飯錢：DeepSeek 餘額和每天花了多少，每把密鑰分開記。

- 每把密鑰各算各的：
  - 聊天回覆：provider_stats 裡每次對話的 token（AstrBot 自己記的，按模型 ID 分開）
  - 後台活（記憶總結、人物卡、覆盤、日報、說說……）：本插件給每個模型的 text_chat 套一層，記下每次呼叫的 token
    和是誰呼叫的（usage.jsonl），聊天回覆那部分已經在 provider_stats 裡，不重複記
  - 都按配置裡的單價、DeepSeek 峰谷時段折算成錢
- 餘額：每小時用每把密鑰查一次 /user/balance。DeepSeek 沒有按密鑰的賬單介面，幾把密鑰屬於同一個帳戶時餘額是共用的，
  這時帳戶的「真實花費」只有一個總數，減去各密鑰算出來的部分就是「沒記到的」（比如別的程式用了這個帳戶）
- 餘額不降反升說明充了錢，那一段按算出來的花費補上
- 畫圖的中轉站沒有賬單介面，按「當天畫了幾張 × 單價」估（單價在插件配置 image_price）
- 每個帳戶餘額低於 alert_at 私信主人提醒充錢，低於一半再提醒一次；充回去之後重新計
- 每小時把彙總寫進 summary.json，面板和日報讀它
主人私信指令：飯錢 / 餘額
```

## 依賴

模型用的是 DeepSeek 官方 API（要查餘額）。畫圖費用要配合 astrbot_plugin_lively_comic 才統計。

## 配置

在 AstrBot WebUI 的插件配置裡填，或者直接編輯 `data/config/astrbot_plugin_lively_billing_config.json`。留空的公共項（主人、群、機器人名字、平台）會用 astrbot_plugin_lively_gate 裡填的。

| 配置項 | 說明 | 預設 |
| --- | --- | --- |
| `providers` | 要記帳的模型：填模型 ID，每個一行。留空自動記所有 api_base 是 deepseek.com 的模型。每把密鑰分開查餘額、分開算花費 | （空） |
| `labels` | 模型的顯示名：一行一個「模型ID=名字」，例如 deepseek-dm=私聊。留空按門禁插件的配置自動起名（哪些群、私聊用它） | （空） |
| `track_all_calls` | 記錄後台呼叫的用量：給每個模型的呼叫套一層，記下記憶總結、人物卡、日報這些後台活各用了哪把密鑰、多少 token。關掉就只能算聊天回覆 | true |
| `alert_at` | 餘額提醒門檻（元）：餘額低於它私信主人一次，再低於一半再提醒一次；填 0 不提醒 | 10.0 |
| `image_price` | 畫圖單價（元/張）：畫圖中轉站沒有賬單介面，按「畫了幾張 × 單價」估算 | 0.3 |
| `price_input_miss` | 輸入單價·未命中快取（元/百萬 tokens）：用來估算聊天花了多少，按你用的模型的官方價填 | 1.0 |
| `price_input_hit` | 輸入單價·命中快取（元/百萬 tokens） | 0.02 |
| `price_output` | 輸出單價（元/百萬 tokens） | 4.0 |
| `peak_double` | 尖峰時段價格翻倍：DeepSeek 官方：工作日 9-12、14-18 點（北京時間）價格翻倍 | true |
| `owner_qq` | 主人 QQ（留空用門禁插件裡填的） | （空） |

## 資料

執行資料在 `data/plugin_data/astrbot_plugin_lively_billing/`。

## 許可

MIT，見 [LICENSE](LICENSE)。

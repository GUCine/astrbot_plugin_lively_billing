# Oblatio · DeepSeek 用量帳本

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
- 按人記：每次聊天回覆是誰觸發的（群裡說話的人、私聊的人）、用了多少 token，記在 users.jsonl；
  面板「饭钱」頁按人匯總（花費、次數、什麼時候來、在哪聊）。開始記之前的私聊從 provider_stats 補，群聊的舊記錄對不上人
- 提示詞留檔（預設關，除錯用）：打開後把〈主人〉私聊每次真的送給模型的整份內容（system、歷史、這一輪、工具定義）
  和模型的回覆、思考存在 prompts/<日期>.jsonl，同一輪調了工具的每一步各一行，AstrBot 壓縮這個私聊的歷史那次也存。
  **會把對話全文存在伺服器上**；system 和工具定義按內容去重存在 prompts/blobs/，圖片只記大小。
  打開幾小時後自動關（prompt_dump_hours），只留最近幾天（prompt_dump_days）
- 按天的帳（ledger）：每天餘額實際掉了多少（兩次查餘額之間掉的按時間攤到每個鐘點）、記到的花在哪幾類、每個鐘點、
  誰、最貴的幾次、快取省了多少、高峰多花多少、降智版要是走 DeepSeek 得花多少；面板「饭钱」頁按天看、點進某一天
- 別的插件在自己開的任務裡調模型時看不出是誰（呼叫棧斷在任務邊界，只看得到 AstrBot），按設定 pid_who
  「模型設定ID=插件名」認（例如群分析插件取話題用的模型設定）
- 錢花在哪（spend）：按做的事分成人看得懂的幾類（群裡聊天、私聊、看圖、整理太長的聊天、回味、記住聊過的事、看手機、
  寫東西、調試），餘額實際掉得比記到的多的那部分單列「沒記到的」（在 AstrBot 外面直接用這個帳戶的，比如伺服器上跑的測試）
- 投餵記錄（feeds.json）：群友給〈我〉的飯錢，只是帳本，不經手錢。〈我〉聊天時聽到了自己記（工具 lively_feed_note，
  記下是誰說的；記錯了用 lively_feed_drop 劃掉自己記的），〈主人〉也能在面板上記、刪
主人私信指令：飯錢 / 餘額；飯錢 留檔 開|關|狀態
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
| `track_users` | 按人記聊天回覆的用量：記下每次聊天回覆是誰觸發的（群裡說話的人、私聊的人）、用了多少 token，面板「饭钱」頁按人看花費和什麼時候來。只記 QQ 號、群名片和 token 數，不記聊天內容 | true |
| `alert_at` | 餘額提醒門檻（元）：餘額低於它私信主人一次，再低於一半再提醒一次；填 0 不提醒 | 10.0 |
| `image_price` | 畫圖單價（元/張）：畫圖中轉站沒有賬單介面，按「畫了幾張 × 單價」估算 | 0.3 |
| `price_input_miss` | 輸入單價·未命中快取（元/百萬 tokens）：用來估算聊天花了多少，按你用的模型的官方價填 | 1.0 |
| `price_input_hit` | 輸入單價·命中快取（元/百萬 tokens） | 0.02 |
| `price_output` | 輸出單價（元/百萬 tokens） | 4.0 |
| `peak_double` | 尖峰時段價格翻倍：DeepSeek 官方：工作日 9-12、14-18 點（北京時間）價格翻倍 | true |
| `prompt_dump` | 提示詞留檔（除錯用）：打開後把主人私聊每次真的送給模型的整份內容（system、歷史、這一輪、工具定義）和回覆、思考存在 plugin_data 的 prompts/ 裡，用來查「哪句提示詞讓她說出哪句話」。會把對話全文存在伺服器上。也可以私信「饭钱 留档 开/关/状态」 | false |
| `prompt_dump_hours` | 留檔開多久自動關（小時）：免得忘了一直開著；填 0 不自動關 | 48 |
| `prompt_dump_days` | 留檔留幾天：每小時把更舊的日檔和沒人引用的 system、工具定義刪掉 | 3 |
| `prompt_dump_qq` | 留誰的私聊：QQ 號，每個一行；留空就是主人。群聊不存（裡面是別人的話） | （空） |
| `prompt_dump_heartbeat` | 留檔也存自主插件的心跳：自主插件自己有 views.jsonl，一般不用開 | false |
| `owner_qq` | 主人 QQ（留空用門禁插件裡填的） | （空） |
| `provider_prices` | 單獨算價的模型設定：不扣 DeepSeek 餘額的模型設定（例如走別家帳號的），一行「模型設定ID=未命中,命中,輸出」元/百萬 tokens；只寫一個 0 就是不算錢，例如「workbuddy-flash=0」。沒寫的都按上面的 DeepSeek 單價算 | （空） |
| `pid_who` | 認不出是誰調的模型時按模型設定認：別的插件在自己開的任務裡調模型時，呼叫棧只看得到 AstrBot、認不出是哪個插件。一行「模型設定ID=插件名」（插件名寫不寫 astrbot_plugin_ 都行），用這個模型設定、又不是 AstrBot 自己看圖或壓縮歷史的，就記成這個插件。例如群分析插件取話題用的模型設定：「deepseek-flash=qq_group_daily_analysis」 | （空） |

## 資料

執行資料在 `data/plugin_data/astrbot_plugin_lively_billing/`。

## 許可

MIT，見 [LICENSE](LICENSE)。

## 取得每日前二十名交易量最大股票資訊存入BigQuery  ##

<ins>說明</ins>: 每天下午兩點從台灣證券交易所(TWSE)下載當日前二十名交易量最大股票，寫入BigQuery。

<ins>流程</ins>: Cloud Scheduler(下午兩點定時啟動) -> 執行Cloud Run -> 下載股票資料 -> 匯入BigQuery

<ins>使用到的雲端資源</ins>： 
 
 - Cloud Shell：建構Docker images並存放到Artifact Registry，佈署Cloud Run服務

 - Cloud Scheduler：設置定時作業，每天下午兩點啟動

 - Artifact Registry：存放Docker images

 - Cloud Run：以Artifact Registry來源的Docker image佈署並執行任務

<ins>步驟</ins>: 

 **(1)** 在BigQuery建立空白表單

   名稱： `daily_top20_stocks`

   schema設定： `bq_table_schema.txt`

   分區欄位： `InDate`

 **(2)** 到Artifact Registry建立repository，名稱為`stock-data-repo`，Format選Docker，地區選asia-east1，`Immutable image tags
 `設定為Enabled

 **(3)** 把這個專案複製到Cloud Shell環境，在`Docker`資料夾下建構image：

 ```
 docker build -t access_top20_stocks_to_bq_img:1.0.0 . --no-cache
 ```

 **(4)** Docker image建構完成後加入tag

 ```
 docker tag access_top20_stocks_to_bq_img:1.0.0 asia-east1-docker.pkg.dev/[PROJECT_NAME]/stock-data-repo/access_top20_stocks_to_bq_img:1.0.0
 ```

 **(5)** 將image推送到Artifact Registry

 ```
 docker push asia-east1-docker.pkg.dev/[PROJECT_NAME]/stock-data-repo/access_top20_stocks_to_bq_img:1.0.0
 ```

 **(6)** 佈署Cloud Run，port設定要和Dockerfile以及code指定的相同

 ```
 gcloud run deploy access-top20-stocks-to-bq-cr --image=asia-east1-docker.pkg.dev/[PROJECT_NAME]/stock-data-repo/access_top20_stocks_to_bq_img:1.0.0 --port=9000 --no-allow-unauthenticated --region=asia-east1
 ```







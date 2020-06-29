extern crate clap;

struct MarketConfig {
    market: String,
    pair: String,
    path_db_file: String,
    periods: String,
    after: i64,
}

fn main() {
    // コマンドライン引数を取得する
    let args_matches = get_args_matches();

    // 取得するマーケットデータを設定する
    let config = configure_market(args_matches);

    // マーケットデータを取得する
    let fetch_result = fetch_market_data_from_cryptowat(&config);

    // 取得に失敗した場合はエラーメッセージを表示して終了する
    if let Err(err) = fetch_result {
        eprintln!("{}", err);
        std::process::exit(1);
    }

    // マーケットデータをデータベースに保存する
    let store_result = store_market_data_to_database(&config, fetch_result.unwrap());

    // 保存に失敗した場合はエラーメッセージを表示して終了する
    if let Err(err) = store_result {
        eprintln!("{}", err);
        std::process::exit(1);
    }

    // 正常終了
    std::process::exit(0);
}

// コマンドライン引数を取得する
fn get_args_matches() -> clap::ArgMatches<'static> {
    clap::App::new("fetch-market-data-rs")
        .version("0.0.1")
        .author("Didy KUPANHY")
        .about("cryptowatから取得したデータをsqliteに保存する")
        .arg(
            clap::Arg::with_name("market")
                .help("対象取引所")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("pair")
                .help("対象通貨")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("periods")
                .help("足の期間(秒指定)")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("path-db-file")
                .help("格納先のDB")
                .takes_value(true)
                .required(true),
        )
        .get_matches()
}

// 取得するマーケットデータを設定する
fn configure_market(args_matches: clap::ArgMatches<'static>) -> MarketConfig {
    let market = args_matches.value_of("market").unwrap().to_string();
    let pair = args_matches.value_of("pair").unwrap().to_string();
    let periods = args_matches.value_of("periods").unwrap().to_string();
    let path_db_file = args_matches.value_of("path-db-file").unwrap().to_string();

    // データベースから指定されたマーケットの最終unixtimeを取得する
    let after = match _get_last_unix_time_from_database(&market, &pair, &periods, &path_db_file) {
        Ok(after) => after,
        Err(err) => {
            eprintln!("{}", err);
            1514764800
        }
    };

    MarketConfig {
        market: market,
        pair: pair,
        path_db_file: path_db_file,
        periods: periods,
        after: after,
    }
}

// マーケットデータをデータベースに保存する
fn _get_last_unix_time_from_database(
    market: &String,
    pair: &String,
    periods: &String,
    path_db_file: &String,
) -> rusqlite::Result<i64> {
    let conn = rusqlite::Connection::open(path_db_file.to_string())?;

    // 最後のunixtime時刻を取得する
    let last_unixtime: i64 = conn.query_row(
        "select COALESCE(max(unixtime), 1514764800) from ohlc where market = ?1 and pair = ?2 and periods = ?3",
        rusqlite::params![&market, &pair, &periods],
        |row| row.get(0)
    )?;

    Ok(last_unixtime)
}

// マーケットデータを取得する
fn fetch_market_data_from_cryptowat(
    config: &MarketConfig,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let market = &config.market;
    let pair = &config.pair;
    let periods = &config.periods;
    let after = &config.after;

    let url = format!(
        "https://api.cryptowat.ch/markets/{}/{}/ohlc?periods={}&after={}",
        market, pair, periods, after
    );
    println!("URL : {}", url);
    let resp = reqwest::blocking::get(&url)?.json::<serde_json::Value>()?;
    Ok(resp)
}

// マーケットデータをデータベースに保存する
fn store_market_data_to_database(
    config: &MarketConfig,
    resp: serde_json::Value,
) -> rusqlite::Result<()> {
    use chrono::{TimeZone, Utc};

    let market = &config.market;
    let pair = &config.pair;
    let periods = &config.periods;
    let path_db_file = &config.path_db_file;
    let after = config.after;

    // UNIX時刻をYYYY-MM-DD hh:mm:ss 形式に変換する
    let len_ohlc = resp["result"][periods.to_string()]
        .as_array()
        .unwrap()
        .len();
    let head_data: chrono::DateTime<Utc> = Utc.timestamp(
        resp["result"][periods.to_string()][0][0].as_i64().unwrap(),
        0,
    );
    let tail_data: chrono::DateTime<Utc> = Utc.timestamp(
        resp["result"][periods.to_string()][len_ohlc - 1][0]
            .as_i64()
            .unwrap(),
        0,
    );

    // 取得したデータの先頭と末尾の日付を出力する
    println!(
        "先頭データ : {}, {}",
        resp["result"][periods.to_string()][0][0].as_i64().unwrap(),
        head_data.to_string()
    );
    println!(
        "末尾データ : {}, {}",
        resp["result"][periods.to_string()][len_ohlc - 1][0]
            .as_i64()
            .unwrap(),
        tail_data.to_string()
    );

    let mut conn = rusqlite::Connection::open(path_db_file.to_string())?;

    // 最終時刻のレコードは再取得するため削除する
    if after != 1514764800 {
        conn.execute(
            "delete from ohlc where market = ?1 and pair = ?2 and periods = ?3 and unixtime = ?4",
            rusqlite::params![&market, &pair, &periods, after],
        )?;
    }

    // 取得したデータをデータベースに保存する
    let sql_key = "market, pair, periods, open, high, low, close, volume, unixtime";
    let sql_value = "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9";
    let sql_insert = &format!("INSERT INTO ohlc ({}) VALUES ({})", sql_key, sql_value);

    let mut lacks = vec![];

    let tx = conn.transaction()?;
    for i in 0..len_ohlc {
        let a_record_unixtime = resp["result"][periods.to_string()][i][0].as_i64().unwrap();
        lacks.push(a_record_unixtime);

        tx.execute(
            sql_insert,
            rusqlite::params![
                market,
                pair,
                periods,
                resp["result"][periods.to_string()][i][1].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][2].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][3].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][4].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][5].as_f64().unwrap(),
                a_record_unixtime,
            ],
        )?;
    }
    tx.commit()?;

    lacks.sort();

    // 欠損データ調査用の変数
    let i_periods = periods.parse::<i64>().unwrap();
    let mut head_unixtime = lacks[0] - i_periods;
    let mut lack = 0;

    // 欠損件数をカウントする
    for i in 0..lacks.len() {
        // 欠損データがあれば出力する
        head_unixtime += i_periods;
        if head_unixtime != lacks[i] {
            let h: chrono::DateTime<Utc> = Utc.timestamp(head_unixtime, 0);
            let t: chrono::DateTime<Utc> = Utc.timestamp(lacks[i], 0);
            let now_lack = (lacks[i] - head_unixtime) / i_periods;
            println!(
                "{}({})から{}({})までの{}件のデータがありません",
                h, head_unixtime, t, lacks[i], now_lack
            );
            head_unixtime = lacks[i];
            lack += now_lack;
        }
    }

    // 保存したデータ数を出力する
    println!(
        "{}件のローソク足データを取得。欠損データは{}件です",
        len_ohlc, lack
    );

    Ok(())
}

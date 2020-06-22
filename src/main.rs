extern crate clap;

struct Config {
    market: String,
    pair: String,
    path_db_file: String,
    periods: String,
    after: String,
}

fn main() {

    // コマンドライン引数を取得する
    let args_matches = get_args_matches();

    // 引数から設定を読み込む
    let config = get_config_from_args(args_matches);

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
                .required(true)
        )
        .arg(
            clap::Arg::with_name("pair")
                .help("対象通貨")
                .takes_value(true)
                .required(true)
        )
        .arg(
            clap::Arg::with_name("path-db-file")
                .help("格納先のDB")
                .takes_value(true)
                .required(true)
        )
        .arg(
            clap::Arg::with_name("period")
                .help("データの時間軸(秒指定)")
                .short("p")
                .long("period")
                .takes_value(true)
                .default_value("900")
        )
        .arg(
            clap::Arg::with_name("after")
                .help("指定されたUNIX日時以降のデータを取得")
                .short("a")
                .long("after")
                .takes_value(true)
                .default_value("1514764800") // 2018/01/01 00:00:00
        )
        .get_matches()
}


// 引数から設定を読み込む
fn get_config_from_args(args_matches: clap::ArgMatches<'static>) -> Config {
    Config {
        market: args_matches.value_of("market").unwrap().to_string(),
        pair: args_matches.value_of("pair").unwrap().to_string(),
        path_db_file: args_matches.value_of("path-db-file").unwrap().to_string(),
        periods: args_matches.value_of("period").unwrap().to_string(),
        after: args_matches.value_of("after").unwrap().to_string(),
    }
}


// マーケットデータを取得する
fn fetch_market_data_from_cryptowat(config: &Config) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let market = &config.market;
    let pair = &config.pair;
    let periods = &config.periods;
    let after = &config.after;

    let url = format!("https://api.cryptowat.ch/markets/{}/{}/ohlc?periods={}&after={}", market, pair, periods, after);
    println!("URL : {}", url);
    let resp = reqwest::blocking::get(&url)?
        .json::<serde_json::Value>()?;
    Ok(resp)
}


// マーケットデータをデータベースに保存する
fn store_market_data_to_database(config: &Config, resp: serde_json::Value) -> rusqlite::Result<()> {
    use chrono::{TimeZone, Utc};

    let market = &config.market;
    let pair = &config.pair;
    let periods = &config.periods;
    let path_db_file = &config.path_db_file;

    let len_ohlc = resp["result"][periods.to_string()].as_array().unwrap().len();
    let head_data: chrono::DateTime<Utc> = Utc.timestamp(resp["result"][periods.to_string()][0][0].as_i64().unwrap(), 0);
    let tail_data: chrono::DateTime<Utc> = Utc.timestamp(resp["result"][periods.to_string()][len_ohlc - 1][0].as_i64().unwrap(), 0);

    println!("先頭データ : {}, {}", resp["result"][periods.to_string()][0][0].as_i64().unwrap(), head_data.to_string());
    println!("末尾データ : {}, {}", resp["result"][periods.to_string()][len_ohlc - 1][0].as_i64().unwrap(), tail_data.to_string());
    println!("{}件のローソク足データを取得", len_ohlc);

    let conn = rusqlite::Connection::open(path_db_file.to_string())?;
    let sql_key = "market, pair, open, high, low, close, volume, unixtime";
    let sql_value = "?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8";
    let sql_insert = &format!("INSERT INTO ohlc ({}) VALUES ({})", sql_key, sql_value);

    for i in 0..len_ohlc {
        conn.execute(
            sql_insert,
            rusqlite::params![
                market,
                pair,
                resp["result"][periods.to_string()][i][1].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][2].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][3].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][4].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][5].as_f64().unwrap(),
                resp["result"][periods.to_string()][i][0].as_i64().unwrap(),
        ])?;
    }

    Ok(())
}

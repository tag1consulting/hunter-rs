use csv::{Reader, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use structopt::StructOpt;
use url::Url;

#[derive(Debug, StructOpt)]
#[structopt(name = "hunter", about = "Read domains from a csv and extract email data from hunter.io.")]
struct Opt {
    /// CSV file to load domains from.
    #[structopt(parse(from_os_str))]
    input: PathBuf,
    /// CSV file to write email to.
    #[structopt(parse(from_os_str))]
    output: PathBuf,
    /// How many records to retreive. (If using a free plan, you must set to 10)
    #[structopt(short, long, default_value="100")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct Domain {
    name: String,
    domain: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Hunter {
    data: Data,
    meta: Meta,
}

#[derive(Debug, Deserialize, Serialize)]
struct Meta {
    results: usize,
    limit: usize,
    offset: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct Data {
    domain: String,
    disposable: bool,
    webmail: bool,
    accept_all: bool,
    pattern: Option<String>,
    organization: Option<String>,
    country: Option<String>,
    state: Option<String>,
    emails: Vec<Email>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    value: String,
    r#type: String,
    confidence: usize,
    sources: Vec<Source>,
    first_name: Option<String>,
    last_name: Option<String>,
    position: Option<String>,
    seniority: Option<String>,
    department: Option<String>,
    linkedin: Option<String>,
    twitter: Option<String>,
    phone_number: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Source {
    domain: String,
    uri: String,
    extracted_on: String,
    last_seen_on: String,
    still_on_page: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Flattened {
    domain: String,
    disposable: bool,
    webmail: bool,
    accept_all: bool,
    pattern: Option<String>,
    organization: Option<String>,
    country: Option<String>,
    state: Option<String>,
    value: String,
    r#type: String,
    confidence: usize,
    first_name: Option<String>,
    last_name: Option<String>,
    position: Option<String>,
    seniority: Option<String>,
    department: Option<String>,
    linkedin: Option<String>,
    twitter: Option<String>,
    phone_number: Option<String>,
}

fn get_api_key() -> String {
    match env::var_os("KEY") {
        Some(val) => {
            val.to_str().unwrap().to_string()
        }
        None => {
            eprintln!("Please set KEY (your hunter.io api key).");
            eprintln!("For example:");
            eprintln!("  KEY=foo cargo run --release -- input.csv output.csv");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = get_api_key();

    let opt = Opt::from_args();

    let mut rdr = Reader::from_path(opt.input)?;
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path(opt.output)?;

    for result in rdr.deserialize() {
        let domain: Domain = result?;
        println!("processing {}...", &domain.domain);
        let hunter_url = Url::parse_with_params("https://api.hunter.io/v2/domain-search",
            &[("domain", &domain.domain), ("api_key", &api_key), ("limit", &opt.limit.to_string())])?;
        let response = reqwest::get(hunter_url).await;
        match response {
            Ok(r) => match r.json::<Hunter>().await {
                Ok(hunter) => {
                    println!("{:?}", hunter.meta);
                    let domain = hunter.data.domain.to_string();
                    let disposable = hunter.data.disposable;
                    let webmail = hunter.data.webmail;
                    let accept_all = hunter.data.accept_all;
                    let pattern = hunter.data.pattern.clone();
                    let organization = hunter.data.organization.clone();
                    let country = hunter.data.country.clone();
                    let state = hunter.data.state.clone();
                    for email in hunter.data.emails {
                        let flattened = Flattened{
                            domain: domain.to_string(),
                            disposable,
                            webmail,
                            accept_all,
                            pattern: pattern.clone(),
                            organization: organization.clone(),
                            country: country.clone(),
                            state: state.clone(),
                            value: email.value,
                            r#type: email.r#type,
                            confidence: email.confidence,
                            first_name: email.first_name,
                            last_name: email.last_name,
                            position: email.position,
                            seniority: email.seniority,
                            department: email.department,
                            linkedin: email.linkedin,
                            twitter: email.twitter,
                            phone_number: email.phone_number,
                        };
                        wtr.serialize(flattened)?;
                    }
                },
                Err(e) => {
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("error: {}", e);
                std::process::exit(1);
            }
        }
    }
    wtr.flush()?;
    Ok(())
}

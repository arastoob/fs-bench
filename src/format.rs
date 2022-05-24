use crate::Error;

pub fn time_format(s: f64) -> String {
    if s * 1e9 < 1000f64 {
        let nano = nano_second(s);
        format!("{} ns", nano)
    } else if s * 1e6 < 1000f64 {
        let micro = micro_second(s);
        format!("{} us", micro)
    } else if s * 1000f64 < 1000f64 {
        let millis = milli_second(s);
        format!("{} ms", millis)
    } else if s < 60f64 {
        format!("{} s", second(s))
    } else if s < 3600f64 {
        let min = (s / 60f64) as i64;
        let second = second(s % 60f64);
        format!("{}:{}", min, second)
    } else {
        let hour = (s / 3600f64) as i64;
        let min = ((s % 3600f64) / 60f64) as i64;
        let second = second(s % 60f64);
        format!("{}:{}:{}", hour, min, second)
    }
}

pub fn time_unit(s: f64) -> &'static str {
    if s * 1e9 < 1000f64 {
        "ns"
    } else if s * 1e6 < 1000f64 {
        "us"
    } else if s * 1000f64 < 1000f64 {
        "ms"
    } else {
        "s"
    }
}

pub fn time_format_by_unit(s: f64, unit: &str) -> Result<f64, Error> {
    match unit {
        "ns" => Ok(nano_second(s)),
        "us" => Ok(micro_second(s)),
        "ms" => Ok(milli_second(s)),
        "s" => Ok(second(s)),
        _ => Err(Error::format(
            "Time conversion",
            format!("invalid time unit: {}", unit),
        )),
    }
}

pub fn percent_format(p: f64) -> String {
    format!("{:.2} %", p)
}

// convert two f64 numbers, which specify the start and end of a range, to scientific notation
pub fn range_format(s: f64, e: f64) -> (f64, f64, &'static str) {
    // we should decide based on the start range number, otherwise our result is incorrect, e.g,
    // (60000, 450000): if we do the convert based on the end ragne number, the result
    // would be (6.0000, 4.50000, 1e4), where the start range becomes bigger than the end range.
    // if the start range number is less than 10, we decide based on the end range number

    // if the end range number if less than 100, don't convert to scientific notation
    if e < 100f64 {
        return (s, e, "1");
    }

    let digit_num = if s < 10f64 {
        e.trunc().to_string().len() // number of digits of the integer part
    } else { s.trunc().to_string().len() };

    return match digit_num {
        1 => { (s, e, "1") },
        2 => { (s / 1e1, e / 1e1, "1e1") },
        3 => { (s / 1e2, e / 1e2, "1e2") },
        4 => { (s / 1e3, e / 1e3, "1e3") },
        5 => { (s / 1e4, e / 1e4, "1e4") },
        6 => { (s / 1e5, e / 1e5, "1e5") },
        7 => { (s / 1e6, e / 1e6, "1e6") },
        8 => { (s / 1e7, e / 1e7, "1e7") },
        9 => { (s / 1e8, e / 1e8, "1e8") },
        _ => { (s / 1e9, e / 1e9, "1e9") }
    }
}

pub fn float_format_by_notation(f: f64, n: &str) -> f64 {
    match n {
        "1" => { f },
        "1e1" => { f / 1e1 },
        "1e2" => { f / 1e2 },
        "1e3" => { f / 1e3 },
        "1e4" => { f / 1e4 },
        "1e5" => { f / 1e5 },
        "1e6" => { f / 1e6 },
        "1e7" => { f / 1e7 },
        "1e8" => { f / 1e8 },
        "1e9" => { f / 1e9 },
        _ => { f },
    }
}

fn micro_second(s: f64) -> f64 {
    let micro = s * 1e6;
    // output to 4 floating points
    (micro * 1e4).trunc() / 1e4
}

fn milli_second(s: f64) -> f64 {
    let millis = s * 1e3;
    // output to 4 floating points
    (millis * 1e4).trunc() / 1e4
}

fn nano_second(s: f64) -> f64 {
    let nano = s * 1e9;
    // output to 4 floating points
    (nano * 1e4).trunc() / 1e4
}

fn second(s: f64) -> f64 {
    // output to 4 floating points
    (s * 1e4).trunc() / 1e4
}

#[cfg(test)]
mod test {
    use crate::format::{time_format, range_format};

    #[test]
    fn time_format_test() {
        assert_eq!(time_format(0.0000000012587), "1.2587 ns".to_string());
        assert_eq!(time_format(0.000000012587), "12.587 ns".to_string());
        assert_eq!(time_format(0.00000012587), "125.87 ns".to_string());
        assert_eq!(time_format(0.0000012587), "1.2587 us".to_string());
        assert_eq!(time_format(0.000012587), "12.587 us".to_string());
        assert_eq!(time_format(0.00012587), "125.87 us".to_string());
        assert_eq!(time_format(0.0012587), "1.2587 ms".to_string());
        assert_eq!(time_format(0.012587), "12.587 ms".to_string());
        assert_eq!(time_format(0.12587), "125.87 ms".to_string());
        assert_eq!(time_format(1258.71542645), "20:58.7154".to_string());

        assert_eq!(time_format(65.126543), "1:5.1265".to_string());
        assert_eq!(time_format(3601.15236), "1:0:1.1523".to_string());
    }

    #[test]
    fn range_format_test() {
        assert_eq!(range_format(60000f64, 460000f64), (6f64, 46f64, "1e4"));
        assert_eq!(range_format(61234f64, 461234f64), (6.1234f64, 46.1234f64, "1e4"));
        assert_eq!(range_format(2354f64, 15147f64), (2.354f64, 15.147f64, "1e3"));
        assert_eq!(range_format(1f64, 127f64), (0.01f64, 1.27f64, "1e2"));
    }
}

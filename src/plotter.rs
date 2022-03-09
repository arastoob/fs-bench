use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use crate::{BenchMode, Error};
use plotters::coord::ranged1d::{DefaultFormatting, KeyPointHint};
use plotters::prelude::*;
use std::fs::File;
use std::ops::Range;
use std::path::PathBuf;

pub struct Plotter {
    x_axis: Vec<XAxis>,
    y_axis: Vec<f64>,
    path: PathBuf,
}

/// The x axis values could be of type float or string
#[derive(Clone)]
enum XAxis {
    STR(String),
    F64(f64),
}

impl From<&str> for XAxis {
    fn from(s: &str) -> Self {
        XAxis::STR(s.to_string())
    }
}

impl From<f64> for XAxis {
    fn from(f: f64) -> Self {
        XAxis::F64(f)
    }
}

impl XAxis {
    pub fn get_str(&self) -> Result<String, Error> {
        match self {
            XAxis::STR(s) => Ok(s.clone()),
            _ => Err(Error::format("x axis", "value is not a string"))
        }
    }

    pub fn get_float(&self) -> Result<f64, Error> {
        match self {
            XAxis::F64(f) => Ok(*f),
            _ => Err(Error::format("x axis", "value is not a float"))
        }
    }
}

impl Plotter {
    pub fn parse(mut path: PathBuf, mode: &BenchMode) -> Result<Self, Error> {
        let file = File::open(path.clone())?;

        // change the filename extension
        path.set_extension("svg");

        match mode {
            BenchMode::OpsPerSecond => {
                let (x_axis, y_axis) = Plotter::parse_ops_per_second(&file)?;
                Ok(Self {
                    x_axis,
                    y_axis,
                    path,
                })
            },
            BenchMode::Behaviour => {
                let (x_axis, y_axis) = Plotter::parse_timestamps(&file)?;
                Ok(Self {
                    x_axis,
                    y_axis,
                    path,
                })
            },
            _ => unimplemented!(),
        }
    }

    pub fn line_chart(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
    ) -> Result<(), Error> {
        let root_area = SVGBackend::new(self.path.as_os_str(), (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        // let custom_x_axes = CustomXAxis::new(self.x_axis.clone());
        let y_min = self.y_axis.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let y_max = self.y_axis.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let y_start = y_min - (y_min / 5.0); // y starts bellow the first y-axis value
        let y_end = y_max + (y_max / 5.0); // and ends after the last y-axis value

        // for the line chart, we need the float values of the x axis
        let x_axis = self.x_axis.iter().map(|x| {
           x.get_float()
        }).collect::<Result<Vec<f64>, Error>>()?;

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 100.0)
            .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
            .caption(caption.unwrap_or(""), ("sans-serif", 40.0))
            .build_cartesian_2d(x_axis[0]..x_axis[x_axis.len() - 1], y_start..y_end)?;

        ctx.configure_mesh()
            .axis_desc_style(("sans-serif", 20.0))
            .x_desc(x_label.unwrap_or(""))
            .y_desc(y_label.unwrap_or(""))
            .draw()?;

        ctx.draw_series(LineSeries::new(
            x_axis
                .iter()
                .zip(self.y_axis.iter())
                .map(|(x, y)| (*x, *y)), // The data iter
            &BLACK,
        ))?;

        let style = ShapeStyle {
            color: BLACK.to_rgba(),
            filled: true,
            stroke_width: 1,
        };

        Ok(())
    }

    pub fn bar_chart(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
    ) -> Result<(), Error> {
        let root_area = SVGBackend::new(self.path.as_os_str(), (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        // for the bar chart, we need the string values of the x axis
        let x_axis = self.x_axis.iter().map(|x| {
            x.get_str()
        }).collect::<Result<Vec<String>, Error>>()?;

        let custom_x_axes = CustomXAxis::new(x_axis);
        let y_min = self.y_axis.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let y_max = self.y_axis.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let y_start = y_min - (y_min / 5.0); // y starts bellow the first y-axis value
        let y_end = y_max + (y_max / 5.0); // and ends after the last y-axis value

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 100.0)
            .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
            .caption(caption.unwrap_or(""), ("sans-serif", 40.0))
            .build_cartesian_2d(custom_x_axes.clone(), y_start..y_end)?;

        ctx.configure_mesh()
            .axis_desc_style(("sans-serif", 20.0))
            .x_desc(x_label.unwrap_or(""))
            .y_desc(y_label.unwrap_or(""))
            .draw()?;

        // draw labels on bars
        ctx.draw_series(PointSeries::of_element(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.y_axis.iter())
                .map(|(x, y)| (x.clone(), *y)),
            5,
            ShapeStyle::from(&RED).filled(),
            &|(x, y), _size, _style| {
                EmptyElement::at((x.clone(), y))
                    + Text::new(format!("{:?}", y), (-20, -10), ("sans-serif", 15))
            },
        ))?;

        // draw the bars
        ctx.draw_series(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.y_axis.iter())
                .map(|(x, y)| {
                    let x_before = format!("{}_before", x);
                    let x_after = format!("{}_after", x);
                    Rectangle::new([(x_before, 0.0), (x_after, *y)], RED.filled())
                }),
        )?;

        Ok(())
    }

    fn parse_ops_per_second(file: &File) -> Result<(Vec<XAxis>, Vec<f64>), Error> {
        let mut reader = csv::Reader::from_reader(file);
        let mut x_axis = vec![];
        let mut y_axis = vec![];

        // find the operation and ops/s columns
        let operation_idx = reader
            .headers()?
            .iter()
            .position(|header| header == "operation")
            .unwrap();
        let ops_per_second_idx = reader
            .headers()?
            .iter()
            .position(|header| header == "ops/s")
            .unwrap();

        for record in reader.records() {
            let record = record?;
            x_axis.push(XAxis::from(record.get(operation_idx).unwrap()));
            y_axis.push(
                record
                    .get(ops_per_second_idx)
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            );
        }

        assert_eq!(x_axis.len(), y_axis.len());

        Ok((x_axis, y_axis))
    }

    fn parse_timestamps(file: &File) -> Result<(Vec<XAxis>, Vec<f64>), Error> {
        let mut reader = csv::Reader::from_reader(file);
        let mut x_axis = vec![];
        let mut y_axis = vec![];

        // find the seconds and ops columns
        let seconds_idx = reader
            .headers()?
            .iter()
            .position(|header| header == "second")
            .unwrap();
        let ops_idx = reader
            .headers()?
            .iter()
            .position(|header| header == "ops")
            .unwrap();

        for record in reader.records() {
            let record = record?;
            x_axis.push(XAxis::from(record.get(seconds_idx).unwrap().parse::<f64>().unwrap()));
            y_axis.push(
                record
                    .get(ops_idx)
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            );
        }

        assert_eq!(x_axis.len(), y_axis.len());

        Ok((x_axis, y_axis))
    }
}

/// Handling the string type values on a plot
#[derive(Clone)]
struct CustomXAxis {
    ticks: Vec<String>,
}

impl CustomXAxis {
    fn new(ticks: Vec<String>) -> Self {
        Self { ticks }
    }
}

impl Ranged for CustomXAxis {
    type ValueType = String;
    type FormatOption = DefaultFormatting;

    fn map(&self, v: &Self::ValueType, pixel_range: (i32, i32)) -> i32 {
        let plot_pixel_range = (pixel_range.1 - pixel_range.0) as usize;
        let tick_distance = plot_pixel_range / self.ticks.len();

        // this case if for calculating the tick position on the plot and for line and point plots
        let pos = self.ticks.iter().position(|tick| tick == v);
        if pos.is_some() {
            return (pos.unwrap() * tick_distance) as i32 + pixel_range.0 + 50;
        }

        // this case and the next one if for calculating the start and end position of a rectangle for bar plot
        let after_pos = self
            .ticks
            .iter()
            .position(|tick| format!("{}_before", tick) == *v);
        if after_pos.is_some() {
            return (after_pos.unwrap() * tick_distance) as i32 + pixel_range.0 + 70;
        }

        let before_pos = self
            .ticks
            .iter()
            .position(|tick| format!("{}_after", tick) == *v);
        if before_pos.is_some() {
            return (before_pos.unwrap() * tick_distance) as i32 + pixel_range.0 + 30;
        }

        return 0;
    }

    fn key_points<Hint: KeyPointHint>(&self, hint: Hint) -> Vec<Self::ValueType> {
        if hint.max_num_points() < 3 {
            vec![]
        } else {
            self.ticks.clone()
        }
    }

    fn range(&self) -> Range<Self::ValueType> {
        self.ticks[0].clone()..self.ticks[self.ticks.len() - 1].clone()
    }
}

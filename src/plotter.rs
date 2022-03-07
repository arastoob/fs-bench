use crate::{BenchMode, Error};
use plotters::coord::ranged1d::{DefaultFormatting, KeyPointHint};
use plotters::prelude::*;
use std::fs::File;
use std::ops::Range;
use std::path::PathBuf;

pub struct Plotter {
    x_axis: Vec<String>,
    y_axis: Vec<f64>,
    path: PathBuf,
}

impl Plotter {
    pub fn parse(path: String, mode: &BenchMode) -> Result<Self, Error> {
        let file = File::open(path.clone())?;
        let (x_axis, y_axis) = match mode {
            BenchMode::OpsPerSecond => Plotter::parse_ops_per_second(&file)?,
            _ => unimplemented!(),
        };

        // change the filename extension
        let (path, _) = path.rsplit_once(".").unwrap();
        let path = PathBuf::from(format!("{}.svg", path));

        Ok(Self {
            x_axis,
            y_axis,
            path,
        })
    }

    pub fn line_chart(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
    ) -> Result<(), Error> {
        let root_area = SVGBackend::new(self.path.as_os_str(), (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        let custom_x_axes = CustomXAxis::new(self.x_axis.clone());
        let y_start = self.y_axis[0] - (self.y_axis[0] / 5.0); // y starts bellow the first y-axis value
        let y_end = self.y_axis[self.y_axis.len() - 1] + (self.y_axis[self.y_axis.len() - 1] / 5.0); // and ends after the last y-axis value

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

        ctx.draw_series(LineSeries::new(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.y_axis.iter())
                .map(|(x, y)| (x.clone(), *y)), // The data iter
            &BLACK,
        ))?;

        let style = ShapeStyle {
            color: BLACK.to_rgba(),
            filled: true,
            stroke_width: 1,
        };
        ctx.draw_series(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.y_axis.iter())
                .map(|(x, y)| Circle::new((x.clone(), *y), 3, style.clone())),
        )?;

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

        let custom_x_axes = CustomXAxis::new(self.x_axis.clone());
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

    fn parse_ops_per_second(file: &File) -> Result<(Vec<String>, Vec<f64>), Error> {
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
            x_axis.push(record.get(operation_idx).unwrap().to_string());
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
}

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

    fn map(&self, v: &String, pixel_range: (i32, i32)) -> i32 {
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

    fn key_points<Hint: KeyPointHint>(&self, hint: Hint) -> Vec<String> {
        if hint.max_num_points() < 3 {
            vec![]
        } else {
            self.ticks.clone()
        }
    }

    fn range(&self) -> Range<String> {
        self.ticks[0].clone()..self.ticks[self.ticks.len() - 1].clone()
    }
}

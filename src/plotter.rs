use std::path::PathBuf;
use std::ops::Range;
use plotters::coord::ranged1d::{DefaultFormatting, KeyPointHint};
use plotters::prelude::*;
use crate::Error;

pub struct Plotter {
    x_axis: Vec<String>,
    y_axis: Vec<f64>,
    path: PathBuf
}

impl Plotter {
    pub fn new(x_axis: Vec<String>, y_axis: Vec<f64>, path: PathBuf) -> Result<Self, Error> {

        if x_axis.len() != y_axis.len() {
            return Err(Error::InvalidConfig("The X and Y axes should be of the same size".to_string()));
        }

        Ok(Self {
            x_axis,
            y_axis,
            path
        })
    }

    pub fn line_chart(&self, x_label: Option<&str>, y_label: Option<&str>, caption: Option<&str>) -> Result<(), Error> {
        let root_area = SVGBackend::new(self.path.as_os_str(), (800, 500))
            .into_drawing_area();
        root_area.fill(&WHITE)?;

        let mut points = vec![];
        for i in 0..self.x_axis.len() {
            points.push((self.x_axis[i].clone(), self.y_axis[i]));
        }

        let custom_x_axes = CustomXAxis::new(self.x_axis.clone());
        let y_start = self.y_axis[0] - (self.y_axis[0] / 5.0); // y starts bellow the first y-axis value
        let y_end = self.y_axis[self.y_axis.len() - 1] + (self.y_axis[self.y_axis.len() - 1] / 5.0); // and ends after the last y-axis value

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 100.0)
            .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
            .caption(caption.unwrap_or(""), ("sans-serif", 40.0))
            .build_cartesian_2d(custom_x_axes, y_start..y_end)?;

        ctx.configure_mesh()
            .axis_desc_style(("sans-serif", 20.0))
            .x_desc(x_label.unwrap_or(""))
            .y_desc(y_label.unwrap_or(""))
            .draw()?;

        ctx.draw_series(
            LineSeries::new(
                points.iter().map(|(x, y)| (x.clone(), *y)), // The data iter
                &BLACK,
            )
        )?;

        let style = ShapeStyle {
            color: BLACK.to_rgba(),
            filled: true,
            stroke_width: 1
        };
        ctx.draw_series(points.iter().map(|(x, y)| Circle::new((x.clone(), *y), 3, style.clone())))?;

        Ok(())
    }
}

struct CustomXAxis {
    ticks: Vec<String>
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
        let index = self.ticks.iter().position(|tick| tick == v).unwrap() + 1;

        (index * tick_distance) as i32 + 50
    }

    fn key_points<Hint:KeyPointHint>(&self, hint: Hint) -> Vec<String> {
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
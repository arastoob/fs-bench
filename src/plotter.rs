use crate::error::Error;
use crate::Record;
use plotters::prelude::*;
use std::ops::Range;
use std::path::Path;

pub struct Plotter {
    coordinates: Vec<Coordinates>,
}

struct Coordinates {
    x_axis: Vec<XAxis>,
    y_axis: Vec<YAxis>,
    label: Option<String>, // the legend label for this series
}

#[derive(Clone)]
struct YAxis {
    y: f64,
    lb: Option<f64>,
    ub: Option<f64>,
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
            _ => Err(Error::format("x axis", "value is not a string")),
        }
    }

    pub fn get_float(&self) -> Result<f64, Error> {
        match self {
            XAxis::F64(f) => Ok(*f),
            _ => Err(Error::format("x axis", "value is not a float")),
        }
    }
}

pub struct Indexes {
    x: usize,
    x_is_str: bool,
    y: usize,
    y_lb: Option<usize>,
    y_ub: Option<usize>,
}

impl Indexes {
    pub fn new(
        x: usize,
        x_is_str: bool,
        y: usize,
        y_lb: Option<usize>,
        y_ub: Option<usize>,
    ) -> Self {
        Self {
            x,
            x_is_str,
            y,
            y_lb,
            y_ub,
        }
    }
}

impl Plotter {
    pub fn new() -> Self {
        Self {
            coordinates: vec![],
        }
    }

    pub fn add_coordinates(
        &mut self,
        records: Vec<Record>,
        label: Option<String>,
        indexes: Indexes,
    ) -> Result<(), Error> {
        let mut x_axis = vec![];
        let mut y_axis = vec![];
        for record in records {
            if indexes.x_is_str {
                x_axis.push(XAxis::from(record.fields[indexes.x].as_str()));
            } else {
                x_axis.push(XAxis::from(record.fields[indexes.x].parse::<f64>()?));
            }

            let lb = if let Some(y_lb) = indexes.y_lb {
                Some(record.fields[y_lb].parse::<f64>()?)
            } else {
                None
            };
            let ub = if let Some(y_ub) = indexes.y_ub {
                Some(record.fields[y_ub].parse::<f64>()?)
            } else {
                None
            };

            y_axis.push(YAxis {
                y: record.fields[indexes.y].parse::<f64>()?,
                lb,
                ub,
            });
        }

        self.coordinates.push(Coordinates {
            x_axis,
            y_axis,
            label,
        });

        Ok(())
    }

    pub fn line_chart<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
        points: bool,
        fixed_ticks: bool,
        file_name: &P,
    ) -> Result<(), Error> {
        // find the min and max values among the coordinates
        let mut y_min = f64::INFINITY;
        let mut y_max = f64::NEG_INFINITY;
        let mut x_max = f64::NEG_INFINITY;
        let mut max_x_axis = vec![]; // the x_axis values that contains the max x value
        for coordinate in self.coordinates.iter() {
            let x_axis = coordinate
                .x_axis
                .iter()
                .map(|x| x.get_float())
                .collect::<Result<Vec<f64>, Error>>()?;
            let max = x_axis.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
            if max > x_max {
                x_max = max;
                max_x_axis = x_axis;
            }

            let y_axis = coordinate
                .y_axis
                .iter()
                .map(|y_axis| y_axis.y)
                .collect::<Vec<_>>();
            let min = y_axis.iter().fold(f64::INFINITY, |a, b| a.min(*b));
            let max = y_axis.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));
            if min < y_min {
                y_min = min;
            }
            if max > y_max {
                y_max = max;
            }
        }

        let y_start = y_min - (y_min / 5.0); // y starts bellow the first y-axis value
        let y_end = y_max + (y_max / 5.0); // and ends after the last y-axis value

        let root_area = SVGBackend::new(file_name, (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        if fixed_ticks {
            let ticks = max_x_axis.iter().map(|x| *x as i64).collect::<Vec<_>>();

            let mut ctx = ChartBuilder::on(&root_area)
                .set_label_area_size(LabelAreaPosition::Left, 100.0)
                .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
                .margin(30.0)
                .caption(caption.unwrap_or(""), ("sans-serif", 30.0))
                .build_cartesian_2d(
                    (ticks[0]..ticks[ticks.len() - 1]).with_key_points(ticks),
                    y_start..y_end,
                )?;

            ctx.configure_mesh()
                .axis_desc_style(("sans-serif", 20.0))
                .x_desc(x_label.unwrap_or(""))
                .x_label_formatter(&|x| {
                    if *x >= 1000 {
                        format!("{:e}", x)
                    } else {
                        x.to_string()
                    }
                })
                .y_desc(y_label.unwrap_or(""))
                .y_label_formatter(&|y| {
                    if *y >= 1000.0 {
                        format!("{:e}", y)
                    } else {
                        y.to_string()
                    }
                })
                .draw()?;

            // plot the coordinates
            let mut has_legend = false;
            let mut colors = (0..).map(Palette99::pick);

            for coordinate in self.coordinates.iter() {
                let x_axis = coordinate
                    .x_axis
                    .iter()
                    .map(|x| x.get_float())
                    .collect::<Result<Vec<f64>, Error>>()?;
                let x_axis = x_axis.iter().map(|x| *x as i64).collect::<Vec<_>>();
                let y_axis = coordinate.y_axis.clone();

                let color = colors.next().unwrap();
                let series = ctx.draw_series(LineSeries::new(
                    x_axis
                        .iter()
                        .zip(y_axis.iter())
                        .map(|(x, y_axis)| (*x, y_axis.y)), // The data iter
                    &color,
                ))?;
                if let Some(label) = coordinate.label.clone() {
                    series
                        .label(label)
                        .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &color));
                    has_legend = true;
                }

                if points {
                    ctx.draw_series(x_axis.iter().zip(coordinate.y_axis.iter()).map(
                        |(x, y_axis)| {
                            Circle::new((*x, y_axis.y), 3, ShapeStyle::from(&BLACK).filled())
                        },
                    ))?;
                }
            }

            if has_legend {
                // draw the legend
                ctx.configure_series_labels().border_style(&BLACK).draw()?;
            }
        } else {
            let mut ctx = ChartBuilder::on(&root_area)
                .set_label_area_size(LabelAreaPosition::Left, 100.0)
                .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
                .margin(30.0)
                .caption(caption.unwrap_or(""), ("sans-serif", 30.0))
                .build_cartesian_2d(0.0..x_max, y_start..y_end)?;

            ctx.configure_mesh()
                .axis_desc_style(("sans-serif", 20.0))
                .x_desc(x_label.unwrap_or(""))
                .x_label_formatter(&|x| {
                    if *x >= 1000.0 {
                        format!("{:e}", x)
                    } else {
                        x.to_string()
                    }
                })
                .y_desc(y_label.unwrap_or(""))
                .y_label_formatter(&|y| {
                    if *y >= 1000.0 {
                        format!("{:e}", y)
                    } else {
                        y.to_string()
                    }
                })
                .draw()?;

            // plot the coordinates
            let mut has_legend = false;
            let mut colors = (0..).map(Palette99::pick);

            for coordinate in self.coordinates.iter() {
                let x_axis = coordinate
                    .x_axis
                    .iter()
                    .map(|x| x.get_float())
                    .collect::<Result<Vec<f64>, Error>>()?;

                let y_axis = coordinate.y_axis.clone();

                let color = colors.next().unwrap();
                let series = ctx.draw_series(LineSeries::new(
                    x_axis
                        .iter()
                        .zip(y_axis.iter())
                        .map(|(x, y_axis)| (*x, y_axis.y)),
                    &color,
                ))?;
                if let Some(label) = coordinate.label.clone() {
                    series
                        .label(label)
                        .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &color));
                    has_legend = true;
                }

                if points {
                    ctx.draw_series(x_axis.iter().zip(coordinate.y_axis.iter()).map(
                        |(x, y_axis)| {
                            Circle::new((*x, y_axis.y), 3, ShapeStyle::from(&BLACK).filled())
                        },
                    ))?;
                }
            }

            if has_legend {
                // draw the legend
                ctx.configure_series_labels().border_style(&BLACK).draw()?;
            }
        };

        // to avoid the IO failure being ignored silently, we manually call the present function
        root_area.present()?;

        Ok(())
    }

    pub fn bar_chart<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
        file_name: &P,
    ) -> Result<(), Error> {
        let root_area = SVGBackend::new(file_name, (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        // for the bar chart, we need the string values of the x axis
        let x_axis = self.coordinates[0]
            .x_axis
            .iter()
            .map(|x| x.get_str())
            .collect::<Result<Vec<String>, Error>>()?;

        let custom_x_axes = CustomXAxis::new(x_axis);
        let y_min = self.coordinates[0]
            .y_axis
            .iter()
            .map(|y_axis| y_axis.y)
            .fold(f64::INFINITY, |a, b| a.min(b));
        let y_max = self.coordinates[0]
            .y_axis
            .iter()
            .map(|y_axis| y_axis.y)
            .fold(f64::NEG_INFINITY, |a, b| a.max(b));
        let y_start = y_min - (y_min / 5.0); // y starts bellow the first y-axis value
        let y_end = y_max + (y_max / 5.0); // and ends after the last y-axis value

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 100.0)
            .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
            .caption(caption.unwrap_or(""), ("sans-serif", 30.0))
            .margin(5.0)
            .build_cartesian_2d(custom_x_axes.clone(), y_start..y_end)?;

        ctx.configure_mesh()
            .axis_desc_style(("sans-serif", 20.0))
            .x_desc(x_label.unwrap_or(""))
            .y_desc(y_label.unwrap_or(""))
            .y_label_formatter(&|y| {
                if *y >= 1000.0 {
                    format!("{:e}", y)
                } else {
                    y.to_string()
                }
            })
            .draw()?;

        // draw the bars
        ctx.draw_series(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.coordinates[0].y_axis.iter())
                .map(|(x, y_axis)| {
                    let x_before = format!("{}_before", x);
                    let x_after = format!("{}_after", x);
                    Rectangle::new([(x_before, 0.0), (x_after, y_axis.y)], RED.filled())
                }),
        )?;

        // draw the error bars
        ctx.draw_series(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.coordinates[0].y_axis.iter())
                .map(|(x, y_axis)| {
                    if let (Some(lb), Some(ub)) = (y_axis.lb, y_axis.ub) {
                        ErrorBar::new_vertical(x.clone(), lb, y_axis.y, ub, BLACK.filled(), 10)
                    } else {
                        ErrorBar::new_vertical(x.clone(), 0f64, 0f64, 0f64, RED.filled(), 0)
                    }
                }),
        )?;

        // draw the bar labels
        let series = ctx.draw_series(
            custom_x_axes
                .ticks
                .iter()
                .zip(self.coordinates[0].y_axis.iter())
                .map(|(x, y_axis)| {
                    EmptyElement::at((x.clone(), y_axis.y))
                        + Text::new(y_axis.y.to_string(), (5, -10), ("sans-serif", 12))
                }),
        )?;

        // draw the legend
        if let Some(label) = self.coordinates[0].label.clone() {
            series
                .label(label)
                .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

            ctx.configure_series_labels().border_style(&BLACK).draw()?;
        }

        // to avoid the IO failure being ignored silently, we manually call the present function
        root_area.present()?;

        Ok(())
    }

    pub fn point_series<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        &self,
        x_label: Option<&str>,
        y_label: Option<&str>,
        caption: Option<&str>,
        file_name: &P,
    ) -> Result<(), Error> {
        let root_area = SVGBackend::new(file_name, (800, 500)).into_drawing_area();
        root_area.fill(&WHITE)?;

        let x_axis = self.coordinates[0]
            .x_axis
            .iter()
            .map(|x| x.get_float())
            .collect::<Result<Vec<f64>, Error>>()?;

        let x_start = x_axis.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        let x_end = x_axis.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b));

        let y_min = self.coordinates[0]
            .y_axis
            .iter()
            .map(|y_axis| y_axis.y)
            .fold(f64::INFINITY, |a, b| a.min(b));
        let y_max = self.coordinates[0]
            .y_axis
            .iter()
            .map(|y_axis| y_axis.y)
            .fold(f64::NEG_INFINITY, |a, b| a.max(b));
        let y_start = y_min - (y_min / 5.0); // y starts bellow the first y-axis value
        let y_end = y_max + (y_max / 5.0); // and ends after the last y-axis value

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 100.0)
            .set_label_area_size(LabelAreaPosition::Bottom, 50.0)
            .caption(caption.unwrap_or(""), ("sans-serif", 30.0))
            .margin(5.0)
            .build_cartesian_2d(x_start..x_end, y_start..y_end)?;

        ctx.configure_mesh()
            .axis_desc_style(("sans-serif", 20.0))
            .x_desc(x_label.unwrap_or(""))
            .x_label_formatter(&|x| {
                if *x >= 1000.0 {
                    format!("{:e}", x)
                } else {
                    x.to_string()
                }
            })
            .y_desc(y_label.unwrap_or(""))
            .y_label_formatter(&|y| {
                if *y >= 1000.0 {
                    format!("{:e}", y)
                } else {
                    y.to_string()
                }
            })
            .draw()?;

        // draw the points
        ctx.draw_series(
            x_axis
                .iter()
                .zip(self.coordinates[0].y_axis.iter())
                .map(|(x, y_axis)| Circle::new((*x, y_axis.y), 2, RED.filled())),
        )?;

        // to avoid the IO failure being ignored silently, we manually call the present function
        root_area.present()?;

        Ok(())
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
    type FormatOption = plotters::coord::ranged1d::DefaultFormatting;

    fn map(&self, v: &Self::ValueType, pixel_range: (i32, i32)) -> i32 {
        let plot_pixel_range = (pixel_range.1 - pixel_range.0) as usize;
        let tick_distance = plot_pixel_range / self.ticks.len();

        // this case if for calculating the tick position on the plot and for line and point plots
        let pos = self.ticks.iter().position(|tick| tick == v);
        if let Some(pos) = pos {
            return (pos * tick_distance) as i32 + pixel_range.0 + 50;
        }

        // this case and the next one if for calculating the start and end position of a rectangle for bar plot
        let after_pos = self
            .ticks
            .iter()
            .position(|tick| format!("{}_before", tick) == *v);
        if let Some(after_pos) = after_pos {
            return (after_pos * tick_distance) as i32 + pixel_range.0 + 70;
        }

        let before_pos = self
            .ticks
            .iter()
            .position(|tick| format!("{}_after", tick) == *v);
        if let Some(before_pos) = before_pos {
            return (before_pos * tick_distance) as i32 + pixel_range.0 + 30;
        }

        return 0;
    }

    fn key_points<Hint: plotters::coord::ranged1d::KeyPointHint>(
        &self,
        hint: Hint,
    ) -> Vec<Self::ValueType> {
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

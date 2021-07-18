// TODO: Make chart generation opt-in
use charts::PointDatum;
use hdrhistogram::Histogram;
use serde::{Serialize, Deserialize};
use charts::{Chart, ScaleLinear, LineSeriesView};

const NANOS_IN_MILLIS: f32 = 1_000_000.0;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FluvioPercentileData {
    pub percentile: f32,
    pub data: f32,
}

impl PointDatum<f32, f32> for FluvioPercentileData {
    fn get_x(&self) -> f32 {
        self.percentile as f32
    }

    fn get_y(&self) -> f32 {
        self.data
    }

    fn get_key(&self) -> String {
        String::new()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FluvioTimeData {
    pub test_elapsed_ms: f32,
    pub data: f32,
}

impl PointDatum<f32, f32> for FluvioTimeData {
    fn get_x(&self) -> f32 {
        self.test_elapsed_ms as f32
    }

    fn get_y(&self) -> f32 {
        self.data
    }

    fn get_key(&self) -> String {
        String::new()
    }
}
pub struct ChartBuilder;

impl ChartBuilder {
    pub fn latency_x_time(
        ts_data: Vec<FluvioTimeData>,
        hist_data: Histogram<u64>,
        title: &str,
        l_axis_label: &str,
        b_axis_label: &str,
        save_path: &str,
    ) {
        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        let x = ScaleLinear::new()
            .set_domain(vec![
                ts_data[0].test_elapsed_ms as f32,
                ts_data.last().unwrap().test_elapsed_ms as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Y-axis will be converted from nanoseconds to milliseconds
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![
                (hist_data.min() as f32 / NANOS_IN_MILLIS),
                (hist_data.max() as f32 / NANOS_IN_MILLIS),
            ])
            .set_range(vec![height - top - bottom, 0]);

        // Transforming data to match y axis
        let line_data = ts_data
            .into_iter()
            .map(|x| FluvioTimeData {
                test_elapsed_ms: x.test_elapsed_ms,
                data: x.data,
            })
            .collect();

        //println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from(title))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label(l_axis_label)
            .add_bottom_axis_label(b_axis_label)
            .save(save_path)
            .unwrap();
    }

    pub fn data_x_time(
        ts_data: Vec<FluvioTimeData>,
        hist_data: Histogram<u64>,
        title: &str,
        l_axis_label: &str,
        b_axis_label: &str,
        save_path: &str,
    ) {
        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        let x = ScaleLinear::new()
            .set_domain(vec![
                ts_data[0].test_elapsed_ms as f32,
                ts_data.last().unwrap().test_elapsed_ms as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Y-axis will be converted from bytes to kilobytes
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![(0 as f32), (hist_data.max() as f32) / 1_000.0])
            .set_range(vec![height - top - bottom, 0]);

        // Transforming data to match y axis
        let line_data = ts_data
            .into_iter()
            .map(|x| FluvioTimeData {
                test_elapsed_ms: x.test_elapsed_ms,
                data: x.data / 1_000.0,
            })
            .collect();

        //println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from(title))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label(l_axis_label)
            .add_bottom_axis_label(b_axis_label)
            .save(save_path)
            .unwrap();
    }

    pub fn mem_usage_x_time(
        ts_data: Vec<FluvioTimeData>,
        hist_data: Histogram<u64>,
        title: &str,
        l_axis_label: &str,
        b_axis_label: &str,
        save_path: &str,
    ) {
        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        let x = ScaleLinear::new()
            .set_domain(vec![
                ts_data[0].test_elapsed_ms as f32,
                ts_data.last().unwrap().test_elapsed_ms as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Y-axis will be converted from kilobytes to megabytes
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![(0 as f32), (hist_data.max() as f32)])
            .set_range(vec![height - top - bottom, 0]);

        // Transforming data to match y axis
        let line_data = ts_data
            .into_iter()
            .map(|x| FluvioTimeData {
                test_elapsed_ms: x.test_elapsed_ms,
                data: x.data,
            })
            .collect();

        //println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from(title))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label(l_axis_label)
            .add_bottom_axis_label(b_axis_label)
            .save(save_path)
            .unwrap();
    }

    pub fn cpu_usage_x_time(
        ts_data: Vec<FluvioTimeData>,
        hist_data: Histogram<u64>,
        title: &str,
        l_axis_label: &str,
        b_axis_label: &str,
        save_path: &str,
    ) {
        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        let x = ScaleLinear::new()
            .set_domain(vec![
                ts_data[0].test_elapsed_ms as f32,
                ts_data.last().unwrap().test_elapsed_ms as f32,
            ])
            .set_range(vec![0, width - left - right]);

        // Y-axis will be converted from nanoseconds to milliseconds
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![(0 as f32), (hist_data.max() as f32)])
            .set_range(vec![height - top - bottom, 0]);

        // Transforming data to match y axis
        let line_data = ts_data
            .into_iter()
            .map(|x| FluvioTimeData {
                test_elapsed_ms: x.test_elapsed_ms,
                data: x.data,
            })
            .collect();

        //println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from(title))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label(l_axis_label)
            .add_bottom_axis_label(b_axis_label)
            .save(save_path)
            .unwrap();
    }

    //
    pub fn latency_x_percentile(
        //ts_data: Vec<FluvioTimeData>,
        hist_data: Histogram<u64>,
        title: &str,
        l_axis_label: &str,
        b_axis_label: &str,
        save_path: &str,
    ) {
        // Define chart related sizes.
        let width = 800;
        let height = 600;
        let (top, right, bottom, left) = (90, 60, 50, 60);

        let x = ScaleLinear::new()
            .set_domain(vec![0 as f32, 100_f32])
            .set_range(vec![0, width - left - right]);

        // Y-axis will be converted from nanoseconds to milliseconds
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![
                (hist_data.value_at_percentile(0.0) as f32) / NANOS_IN_MILLIS,
                (hist_data.value_at_percentile(100.0) as f32) / NANOS_IN_MILLIS,
            ])
            .set_range(vec![height - top - bottom, 0]);

        let mut line_data: Vec<FluvioPercentileData> = Vec::new();

        for p in 0..101 {
            for d in 0..10 {
                let percentile_str = format!("{}.{}", p, d);
                let percentile = percentile_str
                    .parse::<f32>()
                    .expect("Convert string to f32");

                line_data.push(FluvioPercentileData {
                    percentile,
                    data: (hist_data.value_at_percentile(percentile as f64) as f32)
                        / NANOS_IN_MILLIS,
                });

                if (p == 100) && (d == 0) {
                    break;
                }
            }
        }

        //println!("{:?}", &line_data);

        // Create Line series view that is going to represent the data.
        let line_view = LineSeriesView::new()
            .set_x_scale(&x)
            .set_y_scale(&y)
            .set_label_visibility(false)
            .load_data(&line_data)
            .unwrap();

        // Generate and save the chart.
        Chart::new()
            .set_width(width)
            .set_height(height)
            .set_margins(top, right, bottom, left)
            .add_title(String::from(title))
            .add_view(&line_view)
            .add_axis_bottom(&x)
            .add_axis_left(&y)
            .add_left_axis_label(l_axis_label)
            .add_bottom_axis_label(b_axis_label)
            .save(save_path)
            .unwrap();
    }
}

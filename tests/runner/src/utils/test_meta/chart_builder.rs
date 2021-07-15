// TODO: Make chart generation opt-in
use charts::PointDatum;
use hdrhistogram::Histogram;
use serde::{Serialize, Deserialize};
use charts::{Chart, ScaleLinear, LineSeriesView};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FluvioTimeData {
    pub test_elapsed_ms: f32,
    pub data: f32,
}

impl FluvioTimeData {}

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

        // Y-axis will be log scale to account for outliers
        // (Y-axis range is reversed due to SVG's coordinate system)
        let y = ScaleLinear::new()
            .set_domain(vec![
                (hist_data.min() as f32).log(10.0),
                (hist_data.max() as f32).log(10.0),
            ])
            .set_range(vec![height - top - bottom, 0]);

        // Transforming data to match log scale
        let line_data = ts_data
            .clone()
            .into_iter()
            .map(|x| FluvioTimeData {
                test_elapsed_ms: x.test_elapsed_ms,
                data: x.data.log(10.0),
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
}

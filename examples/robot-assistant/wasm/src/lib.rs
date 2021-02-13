mod utils;
mod element;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use element::Element;
use web_sys::Window;
use web_sys::Location;
use web_sys::WebSocket;
use web_sys::ErrorEvent;
use web_sys::MessageEvent;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

fn wsurl() -> Result<String, JsValue> {
    let window: Window = web_sys::window().expect("no global `window` exists");
    let location: Location = window.location();
    let protocol: String = location.protocol()?;
    let host: String = location.host()?;
    let ws_protocol = if protocol == "https:" {
        "wss://"
    } else {
        "ws://"
    };
    Ok(format!("{}{}/ws/", ws_protocol, host))
}

fn process_message(all_messages: &mut Element, message: &str) {
    let mut message_el = Element::create_element("div").unwrap();
    message_el.set_inner_html(message.to_string());
    all_messages.append_child(&mut message_el);
}

#[wasm_bindgen(start)]
pub fn start_websocket() -> Result<(), JsValue> {
    utils::set_panic_hook();

    let wsurl = wsurl()?;
    console_log!("{}", wsurl);

    let mut new_message = Element::qs(".new-message").unwrap();
    let mut all_messages = Element::qs(".all-messages").unwrap();

    let ws = WebSocket::new(&wsurl)?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    let cloned_ws = ws.clone();

    let on_message_change_cb = move |event: web_sys::Event| {
        if let Some(target) = event.target() {
            if let Some(input_el) =
                wasm_bindgen::JsCast::dyn_ref::<web_sys::HtmlInputElement>(&target)
            {
                let v = input_el.value(); // TODO remove with nll
                let txt = v.trim();
                if !txt.is_empty() {
                    input_el.set_value("");
                    cloned_ws.send_with_str(&txt).unwrap();
                    console_log!("{}", txt);
                }
            }
        }
    };
    new_message.add_event_listener("change", on_message_change_cb);

    let onmessage_cb = Closure::wrap(Box::new(move |e: MessageEvent| {
        if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
            let txt: String = txt.into();
            process_message(&mut all_messages, &txt);
            console_log!("message event, received Text: {:?}", txt);
        } else {
            console_log!("message event, received Unknown: {:?}", e.data());
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    ws.set_onmessage(Some(onmessage_cb.as_ref().unchecked_ref()));
    onmessage_cb.forget();

    let onerror_cb = Closure::wrap(Box::new(move |e: ErrorEvent| {
        console_log!("error event: {:?}", e);
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(onerror_cb.as_ref().unchecked_ref()));
    onerror_cb.forget();

    let onopen_cb = Closure::wrap(Box::new(move |_| {
        console_log!("open");
    }) as Box<dyn FnMut(JsValue)>);
    ws.set_onopen(Some(onopen_cb.as_ref().unchecked_ref()));
    onopen_cb.forget();

    Ok(())
}

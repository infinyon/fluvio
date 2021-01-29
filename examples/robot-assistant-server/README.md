## robot-assistent-server

This crate servers the robot assistant assets files in robot-assistant-html and wasm package in robot-assistant-pkg.

After the client create a websocket connection between the server, the robot assitant is created to server text messages and options.

Robot state machine is defined in robot.yaml.

User response is first saved in flovio.io by producer api.
User response is then streamed from flovio.io by comsumer api.
Both user response and robot state are sent to client using websocket for display.


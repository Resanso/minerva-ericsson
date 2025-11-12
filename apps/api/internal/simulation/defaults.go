package simulation

// DefaultSensors returns a baseline set of simulated sensors.
func DefaultSensors() []*Sensor {
	return []*Sensor{
		// Furnace sensors
		NewSensor("Furnace-01", "Temperature", 1200.0, 10.0, 20.0),
		NewSensor("Furnace-01", "Pressure", 100.0, 2.0, 5.0),
		NewSensor("Furnace-02", "Temperature", 1200.0, 10.0, 20.0),
		NewSensor("Furnace-02", "Pressure", 100.0, 2.0, 5.0),
		
		// Rod Feeder sensors
		NewSensor("Rod-Feeder-01", "Temperature", 200.0, 3.0, 8.0),
		NewSensor("Rod-Feeder-01", "Speed", 50.0, 1.0, 3.0),
		
		// UT (Ultrasonic Testing) sensors
		NewSensor("UT-01", "Temperature", 25.0, 1.0, 2.0),
		NewSensor("UT-01", "Accuracy", 98.0, 0.5, 1.0),
		
		// Casting Machine sensors
		NewSensor("Casting-Machine-01", "Temperature", 800.0, 5.0, 10.0),
		NewSensor("Casting-Machine-01", "Pressure", 150.0, 2.0, 5.0),
		NewSensor("Casting-Machine-01", "Speed", 30.0, 1.0, 3.0),
		
		// CT (Cooling Tower) sensors
		NewSensor("CT-01", "Temperature", 80.0, 2.0, 5.0),
		NewSensor("CT-01", "FlowRate", 200.0, 5.0, 10.0),
		NewSensor("CT-02", "Temperature", 80.0, 2.0, 5.0),
		NewSensor("CT-02", "FlowRate", 200.0, 5.0, 10.0),
		NewSensor("CT-03", "Temperature", 80.0, 2.0, 5.0),
		NewSensor("CT-03", "FlowRate", 200.0, 5.0, 10.0),
		NewSensor("CT-04", "Temperature", 80.0, 2.0, 5.0),
		NewSensor("CT-04", "FlowRate", 200.0, 5.0, 10.0),
		
		// Homogenizing sensors
		NewSensor("Homogenizing-01", "Temperature", 500.0, 5.0, 10.0),
		NewSensor("Homogenizing-01", "Pressure", 120.0, 2.0, 5.0),
		
		// Charging Machine sensors
		NewSensor("Charging-Machine-01", "Temperature", 300.0, 3.0, 8.0),
		NewSensor("Charging-Machine-01", "LoadCapacity", 500.0, 10.0, 20.0),
		
		// Swarf sensors
		NewSensor("Swarf-01", "Temperature", 150.0, 2.0, 5.0),
		NewSensor("Swarf-01", "Volume", 100.0, 5.0, 10.0),
		
		// Sawing sensors
		NewSensor("Sawing-01", "Temperature", 100.0, 2.0, 5.0),
		NewSensor("Sawing-01", "BladeSpeed", 200.0, 5.0, 10.0),
		NewSensor("Sawing-01", "Pressure", 80.0, 2.0, 5.0),
		
		// Weightning sensors
		NewSensor("Weightning-01", "Weight", 1000.0, 10.0, 20.0),
		NewSensor("Weightning-01", "Accuracy", 99.5, 0.1, 0.5),
	}
}

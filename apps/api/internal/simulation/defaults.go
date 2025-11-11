package simulation

// DefaultSensors returns a baseline set of simulated sensors.
func DefaultSensors() []*Sensor {
	return []*Sensor{
		NewSensor("Casting-Machine-01", "Temperature", 800.0, 5.0, 10.0),
		NewSensor("Casting-Machine-01", "Pressure", 150.0, 2.0, 5.0),
		NewSensor("Furnace-02", "Temperature", 1200.0, 10.0, 20.0),
	}
}

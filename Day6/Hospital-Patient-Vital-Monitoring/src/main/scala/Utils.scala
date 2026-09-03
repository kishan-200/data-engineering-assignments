object Utils {

  def parseVital(line: String): Option[PatientVital] = {
    try {
      val parts = line.split(",")

      if (parts.length != 7) {
        None
      } else {
        Some(
          PatientVital(
            patientId = parts(0),
            timestamp = parts(1),
            heartRate = parts(2).toDouble,
            spo2 = parts(3).toDouble,
            temperature = parts(4).toDouble,
            systolic = parts(5).toDouble,
            diastolic = parts(6).toDouble
          )
        )
      }
    } catch {
      case _: Exception => None
    }
  }

  def isValid(vital: PatientVital): Boolean = {
    vital.patientId.nonEmpty &&
    vital.timestamp.nonEmpty &&
    vital.heartRate > 0 &&
    vital.spo2 > 0 &&
    vital.temperature > 0 &&
    vital.systolic > 0 &&
    vital.diastolic > 0
  }
}

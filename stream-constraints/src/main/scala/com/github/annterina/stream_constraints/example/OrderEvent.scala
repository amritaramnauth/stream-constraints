package com.github.annterina.stream_constraints.example

import java.util.Date

// Example order update {"key": 92, "date": 1660841981277, "customerId": "egal384r2g", "action": "UPDATED"}
case class OrderEvent(key: Int, date: Date, customerId: String, action: String) {}

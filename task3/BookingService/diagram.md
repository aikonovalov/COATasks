```mermaid
erDiagram
    BOOKING {
        int id PK
        int flight_id "Ext id"

        string passenger_name
        string passenger_email "UNIQUE"

        int seats "> 0"
        bigint total_price "> 0"
        string status "CONFIRMED, CANCELLED"
    }
```

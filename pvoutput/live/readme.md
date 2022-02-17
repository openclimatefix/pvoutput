# Live 

This application pull live data from PV output.org and stores it in our own database


The app has the following high-level strucuture 
```mermaid
  graph TD;
      A[1. Get PV system]-->B;
      B[2. Filter PV Systems]-->C;
      C[3. Pull Data]-->D[4. Save data];
```

1. Get PV System
```mermaid 
   graph TD
    A0(Start) --> A1 
    A0(Start) --> A2 
    A1[Load local PV systems] --> A3{Are all PV system in the database}
    A2[Load Database PV systems] --> A3
    A3 --> |No| A4[Load the extra <br/> PV systems from pvoutput.org]
    A3 --> |yes| A5(Finish)
    A4 --> A5
```

2. Filter PV Systems
```mermaid 
   graph TD
    B0(Start) --> B1{Is there any PV data in <br/> our database for this PV system?}
    B1 --> |No| B2[Keep PV system]
    B1--> |yes| B3{Is there any PV data, <br/> from pv output.org, <br/>available for this PV system?}
    B3 --> |yes| B2
    B3 --> |No| B5[Dischagre PV system]
    B2 --> B6(Finish)
    B5 --> B6
```
3. Pull Data
```mermaid 
   graph TD
    C0(Start) --> C1[Pull Data from pvoutput.prg]
    C1 --> C2{Is this data <br/> in our database already?}
    C2 --> |yes| C3[Keep PV data]
    C2 --> |No| C4[Dischagre PV data]
    C3 --> C5(Finish)
    C4 --> C5
 
```
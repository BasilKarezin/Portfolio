Project:
CRM and Website Database Migration

Problem:
The external contractor discontinued support for the website and its data storage infrastructure, leaving the system without maintenance or ownership. This created risks of service disruption, data inconsistency, and loss of control over critical business processes.

Solution:

Analyzed integration between CRM and the website (Docker-based infrastructure)
Redesigned JSON data exchange logic between systems
Reworked and stabilized file migration scripts
Implemented a new architecture for interaction between JSON, Docker, and MongoDB
Took over full server support, including account management, database administration, backend and frontend maintenance

Additional Details:
Data flow: CRM → Document → API → Website via JSON exchange

Processing API logs and incoming data
Writing data into MongoDB
Replicating data into MS SQL Server via reverse JSON responses

Impact:

Ensured uninterrupted system operation without external contractors
Eliminated critical failure points
Transferred full system ownership to the internal team
Reduced dependency on third-party vendors

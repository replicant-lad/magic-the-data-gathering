# Magic: The Data Gathering
Thanks for checking the project out - the system extracts data from Scryfall's daily bulk dumps and transforms it into a structured format before loading it into a MySQL database for easy access and analysis. For ease (and cost) the database only stores
the most recognizable format of each individual card. The bulk of the work was done using AWS Lambda functions for extracting and transforming the data into an S3 bucket and then loading into the MySQL database from there. A step function is triggered daily at noon MST through an EventBridge.
The project integrates with a boilerplate [LookerStudio Report](https://lookerstudio.google.com/reporting/65b3b7c3-6e47-429f-b331-ec07cfaabce2) that looks at the total number of cards printed over time, and the greatest card price differences from day-to-day and over the week while allowing you to sort by set legality.

You can find the diagrams I built for early modeling below, and can find more of my projects at [Portfolio Site](https://www.replicant.dev/). Feel free to reach out with any questions about the project.

![Blank diagram(5)](https://github.com/replicant-lad/magic-the-data-gathering/assets/5753117/40ac9ca8-d82b-4c67-9fb9-328bcf21976f)
![Untitled-1](https://github.com/replicant-lad/magic-the-data-gathering/assets/5753117/ff2a9d89-dacd-4237-be45-4de8546abc33)

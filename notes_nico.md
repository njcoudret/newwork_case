# Notes Nico

Hi peeps,
Here's my final upload for the case. Of course, I could go on and on and build all kinds of new and extra things, but I think it's enough for now.

I can explain the design tomorrow in more detail, but basically:
- staging contains all beautified, casted and renamed data
- intermediate contains business logic needed to make tables joinable or fix bigger issues in staging data (e.g. countries)
- marts (or bizcore) contains the building blocks, i.e. facts and dims
-- admittedly not necessary necessary for a smaller model like this, but useful when we have many tables and intricate dependencies
-- I like to have this layer as clean and DRY as possible, so no aggregations that would lead to duplicate information. 
- reporting creates wide tables that can directly be imported into Visualisation software such as PowerBI, Looker, Tableau, etc.
-- they contain aggregations or pivotting of columns from other tables but no extra business logic (ideally)
-- i stopped at 3 tables but of course you could go on (opportunities, even more aggregated to date_level, etc. etc.)

I'm looking forward to the interview tomorrow!
Cheers,
Nico
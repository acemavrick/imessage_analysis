# iMessage Analysis
A tool to export and build an sqlite database of desired iMessage conversations.
Builds upon [imessage-exporter](https://github.com/ReagentX/imessage-exporter).

To use, make sure that you have installed `imessage-exporter` into the command line, and that it is in the path.

I used `cargo install imessage-exporter`, but there are different methods of installation, as you can see [here](https://github.com/ReagentX/imessage-exporter/blob/develop/imessage-exporter/README.md).

After installing everything, fill out you address book, with phone numbers and names, in the first notebook cell and set your target. The names do not need to match your contacts- they are used merely as aliases within the notebook itself. The numbers must include the area code (along with the +) and no other characters except numbers.

Finally, run the [jupyter notebook](analysis.ipynb) to see results.

## Known Bugs
There seems to be a bug in the imessage-exporter itself (or my setup), where it doesn't export some messages. Predictably, this skews results, but is not something I know how to fix and has to do with the exporter, not the code in this repository.

## Contributing
Feel free to contribute by making issues, forks, and/or pull-requests.
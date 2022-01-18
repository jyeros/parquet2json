using System.Dynamic;
using Parquet;
using Parquet.Data;

if (args.Length <= 1)
{
    Console.WriteLine("parquet2json <input-parquet-file> <output-json-file>");
    Environment.Exit(1);
}

var parquetPath = args[0];
var outputPath = args[1];

var rowsGroups = new List<DataColumn[]>();

using (Stream fileStream = System.IO.File.OpenRead(parquetPath))
using (var parquetReader = new ParquetReader(fileStream))
{
    DataField[] dataFields = parquetReader.Schema.GetDataFields();
    for (int i = 0; i < parquetReader.RowGroupCount; i++)
        using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
            rowsGroups.Add(dataFields.Select(groupReader.ReadColumn).ToArray());
}

var objs = new List<ExpandoObject>();

foreach (var rowGroup in rowsGroups)
{
    for (int j = 0; j < rowGroup[0].Data.Length; j++)
    {
        var obj = new ExpandoObject();
        for (int i = 0; i < rowGroup.Length; i++)
        {
            obj.TryAdd(rowGroup[i].Field.Name, rowGroup[i].Data.GetValue(j));
        }
        objs.Add(obj);
    }
}

var json = System.Text.Json.JsonSerializer.Serialize(objs, options: new System.Text.Json.JsonSerializerOptions { WriteIndented = true });

File.WriteAllText(outputPath, json);

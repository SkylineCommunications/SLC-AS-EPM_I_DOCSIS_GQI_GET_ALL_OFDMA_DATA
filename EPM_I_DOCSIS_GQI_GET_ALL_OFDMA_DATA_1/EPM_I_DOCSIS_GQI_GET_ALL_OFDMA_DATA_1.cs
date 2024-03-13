/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

13/03/2024	1.0.0.1		GBO, Skyline	Initial version
****************************************************************************
*/

using System;
using System.Collections.Generic;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Messages;
using SLDataGateway.API.Types.Results.Paging;

[GQIMetaData(Name = "All OFDMA Channels Data")]
public class CmData : IGQIDataSource, IGQIInputArguments, IGQIOnInit
{
    private readonly GQIStringArgument frontEndElementArg = new GQIStringArgument("FE Element")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument filterEntityArg = new GQIStringArgument("Filter Entity")
    {
        IsRequired = false,
    };

    private readonly GQIStringArgument entityBeTablePidArg = new GQIStringArgument("BE Entity Table PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityBeCcapIdsArg = new GQIStringArgument("Entity CCAP Dma/Eid IDX")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityBeCmCollectorIdsArg = new GQIStringArgument("Entity Collector Dma/Eid IDX")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityNameCcapPidArg = new GQIStringArgument("Entity Name CCAP PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringDropdownArgument ofdmaInformationArg = new GQIStringDropdownArgument("OFDMA Information", new[] { "OFDMA Channels", "OFDMA Status", "OFDMA Profiles" })
    {
        IsRequired = true,
    };

    private GQIDMS _dms;

    private string frontEndElement = String.Empty;

    private string filterEntity = String.Empty;

    private string ofdmaInformation = String.Empty;

    private int entityBeTablePid = 0;

    private int entityBeCcapIdx = 0;

    private int entityBeCmCollectorIdx = 0;

    private int entityNameCcapPid = 0;

    private List<GQIRow> listGqiRows = new List<GQIRow> { };

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[]
        {
            frontEndElementArg,
            filterEntityArg,
            entityBeTablePidArg,
            entityBeCcapIdsArg,
            entityBeCmCollectorIdsArg,
            entityNameCcapPidArg,
            ofdmaInformationArg,
        };
    }

    public GQIColumn[] GetColumns()
    {
        switch (ofdmaInformation)
        {
            case "OFDMA Channels":
                return new GQIColumn[]
                {
                    new GQIStringColumn("Interface Name"),
                    new GQIIntColumn("Channel ID"),
                    new GQIDoubleColumn("Utilization"),
                    new GQIDoubleColumn("Average RX Power"),
                    new GQIDoubleColumn("Lower Frequency"),
                    new GQIDoubleColumn("Upper Frequency"),
                    new GQIStringColumn("Service Group Name"),
                    new GQIStringColumn("Node Segment Name"),
                    new GQIStringColumn("DS Port Name"),
                };

            case "OFDMA Status":
                return new GQIColumn[]
                {
                    new GQIStringColumn("Interface Name"),
                    new GQIDoubleColumn("Rx Channel Power"),
                    new GQIDoubleColumn("Mean Rx MER"),
                    new GQIDoubleColumn("Standard Deviation Rx MER"),
                    new GQIDoubleColumn("Rx MER Threshold"),
                    new GQIDoubleColumn("Rx MER Value"),
                    new GQIDoubleColumn("Rx MER Highest Frequency"),
                };

            case "OFDMA Profiles":
                return new GQIColumn[]
                {
                    new GQIStringColumn("Interface Name"),
                    new GQIIntColumn("IUC Type"),
                    new GQIDoubleColumn("Corrected Post-FEC"),
                    new GQIDoubleColumn("Uncorrected Post-FEC"),
                };

            default:
                return new GQIColumn[] { };
        }
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        return new GQIPage(listGqiRows.ToArray())
        {
            HasNextPage = false,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        listGqiRows.Clear();
        try
        {
            frontEndElement = args.GetArgumentValue(frontEndElementArg);
            filterEntity = args.GetArgumentValue(filterEntityArg);
            entityBeTablePid = Convert.ToInt32(args.GetArgumentValue(entityBeTablePidArg));
            entityBeCcapIdx = Convert.ToInt32(args.GetArgumentValue(entityBeCcapIdsArg));
            entityBeCmCollectorIdx = Convert.ToInt32(args.GetArgumentValue(entityBeCmCollectorIdsArg));
            entityNameCcapPid = Convert.ToInt32(args.GetArgumentValue(entityNameCcapPidArg));
            ofdmaInformation = args.GetArgumentValue(ofdmaInformationArg);

            var backEndHelper = GetBackEndElement();
            if (backEndHelper == null)
            {
                return new OnArgumentsProcessedOutputArgs();
            }

            var ccapTable = new List<HelperPartialSettings[]>();
            switch (ofdmaInformation)
            {
                case "OFDMA Channels":
                    ccapTable = GetTable(Convert.ToString(backEndHelper.CcapId), 5500, new List<string>
                    {
                        String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityNameCcapPid, filterEntity),
                    });

                    Dictionary<string, CcapOfdmaOverview> ccapRows = ExtractCcapChannelsData(ccapTable);

                    CreateOfdmaRows(ccapRows);

                    break;

                case "OFDMA Status":
                    ccapTable = GetTable(Convert.ToString(backEndHelper.CcapId), 5100, new List<string>
                    {
                        String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityNameCcapPid, filterEntity),
                    });

                    Dictionary<string, CcapOfdmaStatus> ccapStatusRows = ExtractCcapStatusData(ccapTable);

                    CreateOfdmaStatusRows(ccapStatusRows);

                    break;

                case "OFDMA Profiles":
                    ccapTable = GetTable(Convert.ToString(backEndHelper.CcapId), 5150, new List<string>
                    {
                        String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityNameCcapPid, filterEntity),
                    });

                    Dictionary<string, CcapOfdmaProfile> ccapProfileRows = ExtractCcapProfilesData(ccapTable);

                    CreateOfdmaProfileRows(ccapProfileRows);

                    break;

                default:
                    // Do Nothing
                    break;
            }
        }
        catch
        {
            listGqiRows = new List<GQIRow>();
        }

        return new OnArgumentsProcessedOutputArgs();
    }

    public List<HelperPartialSettings[]> GetTable(string element, int tableId, List<string> filter)
    {
        var columns = new List<HelperPartialSettings[]>();

        var elementIds = element.Split('/');
        if (elementIds.Length > 1 && Int32.TryParse(elementIds[0], out int dmaId) && Int32.TryParse(elementIds[1], out int elemId))
        {
            // Retrieve client connections from the DMS using a GetInfoMessage request
            var getPartialTableMessage = new GetPartialTableMessage(dmaId, elemId, tableId, filter.ToArray());
            var paramChange = (ParameterChangeEventMessage)_dms.SendMessage(getPartialTableMessage);

            if (paramChange != null && paramChange.NewValue != null && paramChange.NewValue.ArrayValue != null)
            {
                columns = paramChange.NewValue.ArrayValue
                    .Where(av => av != null && av.ArrayValue != null)
                    .Select(p => p.ArrayValue.Where(v => v != null)
                    .Select(c => new HelperPartialSettings
                    {
                        CellValue = c.CellValue.InteropValue,
                        DisplayValue = c.CellValue.CellDisplayValue,
                        DisplayType = c.CellDisplayState,
                    }).ToArray()).ToList();
            }
        }

        return columns;
    }

    public BackEndHelper GetBackEndElement()
    {
        if (String.IsNullOrEmpty(filterEntity))
        {
            return null;
        }

        var backendTable = GetTable(frontEndElement, 1200500, new List<string>
        {
            "forceFullTable=true",
        });

        if (backendTable != null && backendTable.Any())
        {
            for (int i = 0; i < backendTable[0].Count(); i++)
            {
                var key = Convert.ToString(backendTable[0][i].CellValue);

                var backendEntityTable = GetTable(key, entityBeTablePid, new List<string>
                {
                    String.Format("forceFullTable=true;fullFilter=({0}=={1})", entityBeTablePid + 2, filterEntity),
                });

                if (backendEntityTable != null && backendEntityTable.Any() && backendEntityTable[0].Length > 0)
                {
                    return new BackEndHelper
                    {
                        ElementId = key,
                        CcapId = Convert.ToString(backendEntityTable[entityBeCcapIdx][0].CellValue),
                        CollectorId = Convert.ToString(backendEntityTable[entityBeCmCollectorIdx][0].CellValue),
                        EntityId = Convert.ToString(backendEntityTable[0][0].CellValue),
                    };
                }
            }
        }

        return null;
    }

    public string ParseDoubleValue(double doubleValue, string unit)
    {
        if (doubleValue.Equals(-1))
        {
            return "N/A";
        }

        return Math.Round(doubleValue, 2) + " " + unit;
    }

    public string ParseStringValue(string stringValue)
    {
        if (String.IsNullOrEmpty(stringValue) || stringValue == "-1")
        {
            return "N/A";
        }

        return stringValue;
    }

    private static Dictionary<string, CcapOfdmaOverview> ExtractCcapChannelsData(List<HelperPartialSettings[]> ccapTable)
    {
        Dictionary<string, CcapOfdmaOverview> ccapRows = new Dictionary<string, CcapOfdmaOverview>();
        if (ccapTable != null && ccapTable.Any())
        {
            for (int i = 0; i < ccapTable[0].Count(); i++)
            {
                var key = Convert.ToString(ccapTable[0][i].CellValue);
                var ccapRow = new CcapOfdmaOverview
                {
                    OfdmaId = key,
                    OfdmaInterfaceName = Convert.ToString(ccapTable[2][i].CellValue),
                    OfdmaChannelId = Convert.ToInt32(ccapTable[3][i].CellValue),
                    OfdmaUtilization = Convert.ToDouble(ccapTable[4][i].CellValue),
                    OfdmaRxPower = Convert.ToDouble(ccapTable[5][i].CellValue),
                    OfdmaLowerFrequency = Convert.ToDouble(ccapTable[12][i].CellValue),
                    OfdmaUpperFrequency = Convert.ToDouble(ccapTable[13][i].CellValue),
                    OfdmaNodeSegmentName = Convert.ToString(ccapTable[8][i].CellValue),
                    OfdmaServiceGroupName = Convert.ToString(ccapTable[6][i].CellValue),
                    OfdmaUsPortName = Convert.ToString(ccapTable[10][i].CellValue),
                };

                ccapRows[key] = ccapRow;
            }
        }

        return ccapRows;
    }

    private static Dictionary<string, CcapOfdmaStatus> ExtractCcapStatusData(List<HelperPartialSettings[]> ccapTable)
    {
        Dictionary<string, CcapOfdmaStatus> ccapRows = new Dictionary<string, CcapOfdmaStatus>();
        if (ccapTable != null && ccapTable.Any())
        {
            for (int i = 0; i < ccapTable[0].Count(); i++)
            {
                var key = Convert.ToString(ccapTable[0][i].CellValue);
                var ccapRow = new CcapOfdmaStatus
                {
                    OfdmaId = key,
                    OfdmaInterfaceName = Convert.ToString(ccapTable[7][i].CellValue),
                    OfdmaRxPower = Convert.ToDouble(ccapTable[1][i].CellValue),
                    OfdmaMeanMer = Convert.ToDouble(ccapTable[2][i].CellValue),
                    OfdmaStandardDeviationMer = Convert.ToDouble(ccapTable[3][i].CellValue),
                    OfdmaMerThreshold = Convert.ToDouble(ccapTable[4][i].CellValue),
                    OfdmaMerValue = Convert.ToDouble(ccapTable[5][i].CellValue),
                    OfdmaMerHighestFrequency = Convert.ToDouble(ccapTable[6][i].CellValue),
                };

                ccapRows[key] = ccapRow;
            }
        }

        return ccapRows;
    }

    private static Dictionary<string, CcapOfdmaProfile> ExtractCcapProfilesData(List<HelperPartialSettings[]> ccapTable)
    {
        Dictionary<string, CcapOfdmaProfile> ccapRows = new Dictionary<string, CcapOfdmaProfile>();
        if (ccapTable != null && ccapTable.Any())
        {
            for (int i = 0; i < ccapTable[0].Count(); i++)
            {
                var key = Convert.ToString(ccapTable[0][i].CellValue);
                var ccapRow = new CcapOfdmaProfile
                {
                    OfdmaId = key,
                    OfdmaInterfaceName = Convert.ToString(ccapTable[9][i].CellValue),
                    OfdmaIucType = Convert.ToInt32(ccapTable[11][i].CellValue),
                    OfdmaCorrectedPostFec = Convert.ToDouble(ccapTable[4][i].CellValue),
                    OfdmaUnCorrectedPostFec = Convert.ToDouble(ccapTable[5][i].CellValue),
                };

                ccapRows[key] = ccapRow;
            }
        }

        return ccapRows;
    }

    private void CreateOfdmaRows(Dictionary<string, CcapOfdmaOverview> ccapRows)
    {
        foreach (var ccapRow in ccapRows)
        {
            List<GQICell> listGqiCells = new List<GQICell>
                {
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaInterfaceName,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaChannelId,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaUtilization,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaUtilization, "%"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaRxPower,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaRxPower, "dBmV"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaLowerFrequency,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaLowerFrequency, "MHz"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaUpperFrequency,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaUpperFrequency, "MHz"),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmaServiceGroupName),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmaNodeSegmentName),
                    },
                    new GQICell
                    {
                        Value = ParseStringValue(ccapRow.Value.OfdmaUsPortName),
                    },
                };

            var gqiRow = new GQIRow(listGqiCells.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }

    private void CreateOfdmaStatusRows(Dictionary<string, CcapOfdmaStatus> ccapRows)
    {
        foreach (var ccapRow in ccapRows)
        {
            List<GQICell> listGqiCells = new List<GQICell>
                {
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaInterfaceName,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaRxPower,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaRxPower, "dBmV"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaMeanMer,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaMeanMer, "dB"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaStandardDeviationMer,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaStandardDeviationMer, "dB"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaMerThreshold,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaMerThreshold, "%"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaMerValue,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaMerValue, "dB"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaMerHighestFrequency,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaMerHighestFrequency, "MHz"),
                    },
                };

            var gqiRow = new GQIRow(listGqiCells.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }

    private void CreateOfdmaProfileRows(Dictionary<string, CcapOfdmaProfile> ccapRows)
    {
        foreach (var ccapRow in ccapRows)
        {
            List<GQICell> listGqiCells = new List<GQICell>
                {
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaInterfaceName,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaIucType,
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaCorrectedPostFec,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaCorrectedPostFec, "%"),
                    },
                    new GQICell
                    {
                        Value = ccapRows[ccapRow.Key].OfdmaUnCorrectedPostFec,
                        DisplayValue = ParseDoubleValue(ccapRows[ccapRow.Key].OfdmaUnCorrectedPostFec, "ppm"),
                    },
                };

            var gqiRow = new GQIRow(listGqiCells.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }
}

public class BackEndHelper
{
    public string ElementId { get; set; }

    public string CcapId { get; set; }

    public string CollectorId { get; set; }

    public string EntityId { get; set; }
}

public class HelperPartialSettings
{
    public object CellValue { get; set; }

    public object DisplayValue { get; set; }

    public ParameterDisplayType DisplayType { get; set; }
}

public class CcapOfdmaOverview
{
    public string OfdmaId { get; set; }

    public string OfdmaInterfaceName { get; set; }

    public int OfdmaChannelId { get; set; }

    public double OfdmaUtilization { get; set; }

    public double OfdmaRxPower { get; set; }

    public double OfdmaLowerFrequency { get; set; }

    public double OfdmaUpperFrequency { get; set; }

    public string OfdmaServiceGroupName { get; set; }

    public string OfdmaNodeSegmentName { get; set; }

    public string OfdmaUsPortName { get; set; }
}

public class CcapOfdmaStatus
{
    public string OfdmaId { get; set; }

    public string OfdmaInterfaceName { get; set; }

    public double OfdmaRxPower { get; set; }

    public double OfdmaMeanMer { get; set; }

    public double OfdmaStandardDeviationMer { get; set; }

    public double OfdmaMerThreshold { get; set; }

    public double OfdmaMerValue { get; set; }

    public double OfdmaMerHighestFrequency { get; set; }
}

public class CcapOfdmaProfile
{
    public string OfdmaId { get; set; }

    public string OfdmaInterfaceName { get; set; }

    public int OfdmaIucType { get; set; }

    public double OfdmaCorrectedPostFec { get; set; }

    public double OfdmaUnCorrectedPostFec { get; set; }
}
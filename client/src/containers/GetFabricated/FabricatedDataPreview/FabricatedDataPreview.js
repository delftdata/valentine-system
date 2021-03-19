import React, {Component} from "react";
// import classes from './FabricatedDataPreview.module.css'
import SimpleTable from '../../../components/UI/SimpleTable/SimpleTable'

const source_head = ["Fiscal year", "Project number", "Status", "Maximum CIDA contribution (project-level)", "Branch ID",
    "Branch name", "Division ID", "Division name", "Section ID", "Section name", "Regional program (marker)",
    "Fund centre ID", "Fund centre name", "Untied amount(Project-level budget)", "FSTC percent", "IRTC percent",
    "CFLI (marker)", "CIDA business delivery model (old)", "Programming process (new)", "Bilateral aid (international marker)",
    "PBA type", "Enviromental sustainability (marker)", "Climate change adaptation (marker)", "Climate change mitigation (marker)",
    "Desertification (marker)", "Participatory development and good governance"]

const source_body = [["200o2010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                        ["20p92010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                        ["2009q010", "A017716001", "Closed", "8500267.65", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4122", "Bangladesh Section", "0", "4122", "Bangladesh", "8500267.65", "1.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                        ["20992010", "A018652001", "Closed", "500458.33", "B3100", "EGM Europe", " Middle East and Maghreb", "D3100", "EDD Europe-Middle East Programming", "S4264", "West Bank Gaza & Palestinian Refugees", "0", "4265", "West Bank Gaza", "0.0", "0.0", "0.0", "0", "Directive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "0"],
                        ["2p092010", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                        ["2009201o", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                        ["200920w0", "A018893001", "Closed", "14410076.46", "B4200", "WGM Sub-Saharan Africa", "D4207", "WWD West & Central Africa", "S4215", "Mali Program Section", "0", "4216", "Mali", "14410076.46", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                        ["20092p10", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan, Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                        ["20092019", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan, Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"],
                        ["200o2010", "A019043001", "Closed", "13743220.56", "B4100", "OGM Asia Pacific", "D4100", "OAD Asia Programming", "S4141", "OAA Afghanistan, Pakistan and Sri Lanka", "0", "4124", "Pakistan", "13743220.56", "1.0", "0.0", "0", "Responsive", "Uncoded: Pre-APP", "1", "Not PBA", "0", "0", "0", "0", "1"]]

const target_head = ["miller2_FiscalYear", "miller2_TradeDevelopment(marker)", "miller2_Biodiversity(marker)",
    "miller2_UrbanIssues(marker)", "miller2_ChildrenIssues(marker)", "miller2_YouthIssues(marker)", "miller2_IndigenousIssues(marker)",
    "miller2_DisabilityIssues(marker)", "miller2_ICTAsAToolForDevelopment(marker)", "miller2_KnowledgeForDevelopment(marker)",
    "miller2_GenderEquality(marker)", "miller2_OrganisationID", "miller2_OrganisationName", "miller2_OrganisationType",
    "miller2_OrganisationClass", "miller2_OrganisationSub-class", "miller2_ContinentID", "miller2_ContinentName", "miller2_ProjectBrowserCountryID",
    "miller2_CountryRegionID", "miller2_CountryRegionName", "miller2_CountryRegionPercent", "miller2_SectorID", "miller2_SectorName",
    "miller2_SectorPercent", "miller2_AmountSpent"]


const target_body = [["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "33140", "Multilateral trade negotiations", "0.14", "75768.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "3", "Asia", "X3", "X3", "Asia MC", "0.22", "99810", "Sectors not specified", "0.505", "273306.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "12191", "Medical services", "0.268", "210969.6"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "14015", "Water resources conservation (including data collection)", "0.047", "36998.4"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "23510", "Nuclear energy electric power plants", "0.04", "31488.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "33140", "Multilateral trade negotiations", "0.14", "110208.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "1", "1", "0", "1005967", "IAEA - International Atomic Energy Agency Technical Cooperation Fund", "Foreign Non-Profit Making", "Multilateral", "UNITED NATIONS", "5", "Europe", "X4", "X4", "Europe MC", "0.32", "99810", "Sectors not specified", "0.505", "397536.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1001300", "PAHO - Pan American Health Organization", "Foreign Non-Profit Making", "Multilateral", "REGIONAL ORGANIZATION", "1", "America", "SV", "SV", "El Salvador", "1.0", "72010", "Material relief assistance and services", "1.0", "300000.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "2", "Africa", "X1", "X1", "Africa MC", "0.5", "72010", "Material relief assistance and services", "1.0", "1500000.0"],
                        ["2009/2010", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1014663", "ICRC Appeals via CRCS", "Canadian Non-Profit Making", "Civil Society", "INTERNATIONAL NGO", "1", "America", "X2", "X2", "Americas MC", "0.15", "72010", "Material relief assistance and services", "1.0", "450000.0"]]

class  FabricatedDataPreview extends Component {

    render() {
        return(
            <div>
                <div>
                    <h5>Table 1:</h5>
                    <SimpleTable head={source_head} body={source_body}/>
                </div>
                <div>
                    <h5>Table 2:</h5>
                    <SimpleTable head={target_head} body={target_body}/>
                </div>
            </div>
        );
    }

}

export default FabricatedDataPreview;
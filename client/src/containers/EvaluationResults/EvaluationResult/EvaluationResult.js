import React, {Component} from "react";
import Aux from "../../../hoc/Aux";
import Modal from "../../../components/UI/Modal/Modal";
import Spinner from "../../../components/UI/Spinner/Spinner";
import classes from "./EvaluationResult.module.css";
import Button from "@material-ui/core/Button";
import GetAppIcon from "@material-ui/icons/GetApp";
import {TableContainer} from "@material-ui/core";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableFooter from "@material-ui/core/TableFooter";
import TableRow from "@material-ui/core/TableRow";
import TablePagination from "@material-ui/core/TablePagination";
import SimpleTable from "../../../components/UI/SimpleTable/SimpleTable";


const spurious_head = ["Algorithm", "Column 1", "Column 2", "Similarity", "Type"]

const spurious_body = [
    ["Cupid", "Climate change adaptation (marker)", "miller2_TradeDevelopment(marker)", "0.932", "False Positive"],
    ["EmbDI", "Desertification (marker)", "miller2_ChildrenIssues(marker)", "0.757", "False Positive"],
    ["EmbDI", "Desertification (marker)", "miller2_UrbanIssues(marker)", "0.755", "False Positive"],
    ["EmbDI", "Climate change mitigation (marker)", "miller2_Biodiversity(marker)", "0.754", "False Positive"],
    ["EmbDI", "Climate change mitigation (marker)", "miller2_Biodiversity(marker)", "0.754", "False Positive"],
    ["EmbDI", "Climate change mitigation (marker)", "miller2_Biodiversity(marker)", "0.754", "False Positive"],
    ["EmbDI", "Climate change mitigation (marker)", "miller2_Biodiversity(marker)", "0.754", "False Positive"],
]

class EvaluationResult extends Component {

    state = {
        loading: false,
        page: 0,
        rowsPerPage: 5,
        showSpurious: {},
        spurious: {},
    }

    componentDidMount() {
        const spurious = {};
        const showSpurious = {};
        console.log(this.props.pairIds)
        for (const pairId of this.props.pairIds) {
            spurious[pairId] = {};
            showSpurious[pairId] = false;
        }
        this.setState({showSpurious: showSpurious, spurious:spurious});
    }

    downloadDataset = (fabricatedPairId) => {

    }

    showSpuriousResults = (fabricatedPairId) => {
        const showSpurious = {...this.state.showSpurious};
        showSpurious[fabricatedPairId] = !showSpurious[fabricatedPairId];
        this.setState({showSpurious: showSpurious});
    }

    handleChangePage = (event, newPage) => {
        this.setState({page: newPage});
    };

    handleChangeRowsPerPage = (event) => {
        this.setState({rowsPerPage: +event.target.value});
        this.setState({page: 0});
    };

    render() {
        return(
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <div className={classes.Parent}>
                    <TableContainer className={classes.Container}>
                        <Table className={classes.Results}>
                            <TableBody>
                                {this.props.pairIds.slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((datasetId) => {
                                        const spuriousResults = this.state.showSpurious[datasetId] ?
                                                <div>
                                                    <SimpleTable head={spurious_head} body={spurious_body}/>
                                                </div>
                                                : null;
                                        return (<div className={classes.FabricatedPair}>
                                                    <p>Fabricated pair: {datasetId}</p>
                                                    <Button
                                                        style={{
                                                            borderRadius: 10,
                                                            color: "#016b9f",
                                                            padding: "10px 10px",
                                                            fontSize: "8px"
                                                        }}
                                                        onClick={() => this.downloadDataset(datasetId)}>
                                                        <GetAppIcon/>
                                                    </Button>
                                                    <Button
                                                        style={{
                                                            borderRadius: 10,
                                                            color: "white",
                                                            padding: "10px 10px",
                                                            marginLeft: "10px",
                                                            fontSize: "10px",
                                                            background: "#71100f"
                                                        }}
                                                        onClick={() => this.showSpuriousResults(datasetId)}>
                                                        Show Spurious Results
                                                    </Button>
                                                    <div className={classes.Sample}>
                                                        {spuriousResults}
                                                    </div>
                                                </div>);
                                        }
                                    )
                                }
                            </TableBody>
                            <TableFooter>
                                <TableRow>
                                    <div className={classes.Pagination}>
                                        <TablePagination
                                        rowsPerPageOptions={[5, 10, 25]}
                                        component="div"
                                        count={this.props.pairIds.length}
                                        rowsPerPage={this.state.rowsPerPage}
                                        page={this.state.page}
                                        onChangePage={this.handleChangePage}
                                        onChangeRowsPerPage={this.handleChangeRowsPerPage}
                                        />
                                    </div>
                                </TableRow>
                            </TableFooter>
                        </Table>
                    </TableContainer>
                </div>
            </Aux>
        );
    }

}

export default EvaluationResult;
import React, {Component} from "react";
import classes from "./Dataset.module.css";
import Button from "@material-ui/core/Button";
import GetAppIcon from "@material-ui/icons/GetApp";
import DeleteIcon from "@material-ui/icons/Delete";
import Aux from "../../../hoc/Aux";
import Modal from "../../../components/UI/Modal/Modal";
import Spinner from "../../../components/UI/Spinner/Spinner";
import {TableContainer} from "@material-ui/core";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableFooter from "@material-ui/core/TableFooter";
import TableRow from "@material-ui/core/TableRow";
import TablePagination from "@material-ui/core/TablePagination";
import FabricatedDataPreview from "../FabricatedDataPreview/FabricatedDataPreview";

class Dataset extends Component {

    state = {
        showSamples: {},
        samples: {},
        loading: false,
        page: 0,
        rowsPerPage: 5,
    }

    componentDidMount() {
        const samples = {};
        const showSamples = {};
        console.log(this.props.pairIds)
        for (const pairId of this.props.pairIds) {
            samples[pairId] = {};
            showSamples[pairId] = false;
        }
        this.setState({showSamples: showSamples, samples:samples});
    }

    showSample = (fabricatedPairId) => {
        const showSamples = {...this.state.showSamples};
        showSamples[fabricatedPairId] = !showSamples[fabricatedPairId];
        this.setState({showSamples: showSamples});
    }

    downloadDataset = (fabricatedPairId) => {

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
                                                const fabricatedDataPreview = this.state.showSamples[datasetId] ?
                                                <FabricatedDataPreview sample={this.state.samples[datasetId]}/>
                                                : null;
                                        return (<div className={classes.FabricatedPair}>
                                            <p>Fabricated pair: {datasetId}</p>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    backgroundColor: "#016b9f",
                                                    color: "white",
                                                    padding: "10px 10px",
                                                    fontSize: "11px"
                                                }}
                                                onClick={() => this.showSample(datasetId)}>
                                                Show Sample
                                            </Button>
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
                                                    color: "#71100f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px"
                                                }}
                                                onClick={() => this.deleteDataset(datasetId)}>
                                                <DeleteIcon/>
                                            </Button>
                                            <div className={classes.Sample}>
                                                {fabricatedDataPreview}
                                            </div>
                                        </div>);})
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

export default Dataset;
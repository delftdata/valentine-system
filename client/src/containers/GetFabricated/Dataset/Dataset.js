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
import axios from "axios";

const FileSaver = require('file-saver');

class Dataset extends Component {

    state = {
        showSamples: {},
        loading: false,
        page: 0,
        rowsPerPage: 5,
        datasetGroupPairs: []
    }

    componentDidMount() {
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/get_fabricated_dataset_group_pairs/" +
                 this.props.datasetId
        }).then(res => {
            const showSamples = {};
            for (const pairId of res.data) {
                showSamples[pairId] = false;
            }
            this.setState({datasetGroupPairs: res.data, showSamples: showSamples});
        }).catch(err => {
            console.log(err);
        })
    }

    showSample = (fabricatedPairId) => {
        const showSamples = {...this.state.showSamples};
        showSamples[fabricatedPairId] = !showSamples[fabricatedPairId];
        this.setState({showSamples: showSamples});
    }

    downloadDataset = (fabricatedPairId) => {
        this.setState({loading: true});
        axios({
            method: "get",
            url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/download_fabricated_dataset_pair/" +
            this.props.datasetId + "/" + fabricatedPairId,
            responseType: 'blob',
        }).then(res => {
            this.setState({loading: false});
            FileSaver.saveAs(res.data, fabricatedPairId + '.zip');
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    deleteDataset = (fabricatedPairId, index) => {
        axios.delete( process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/delete_fabricated_dataset_pair/" +
            this.props.datasetId + "/" + fabricatedPairId
        ).then(() => {
            const datasetGroupPairs = [...this.state.datasetGroupPairs]
            datasetGroupPairs.splice(index, 1)
            this.setState({datasetGroupPairs: datasetGroupPairs});
        }).catch(err => {
            console.log(err);
        })
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
                                {this.state.datasetGroupPairs.slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((datasetPairId, index) => {
                                                const fabricatedDataPreview = this.state.showSamples[datasetPairId] ?
                                                <FabricatedDataPreview datasetId={this.props.datasetId}
                                                                       pairId={datasetPairId}/>
                                                : null;
                                        return (<div className={classes.FabricatedPair}>
                                            <p>Fabricated pair: {datasetPairId}</p>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    backgroundColor: "#016b9f",
                                                    color: "white",
                                                    padding: "10px 10px",
                                                    fontSize: "11px"
                                                }}
                                                onClick={() => this.showSample(datasetPairId)}>
                                                Show Sample
                                            </Button>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    color: "#016b9f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px"
                                                }}
                                                onClick={() => this.downloadDataset(datasetPairId)}>
                                                <GetAppIcon/>
                                            </Button>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    color: "#71100f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px"
                                                }}
                                                onClick={() => this.deleteDataset(datasetPairId, index)}>
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
                                        count={this.state.datasetGroupPairs.length}
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
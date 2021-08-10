import React, {Component} from "react";
import Aux from "../../hoc/Aux";
import axios from "axios";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import classes from "./GetFabricated.module.css";
import {TableContainer} from "@material-ui/core";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import Button from "@material-ui/core/Button";
import TablePagination from "@material-ui/core/TablePagination";
import GetAppIcon from '@material-ui/icons/GetApp';
import TableFooter from "@material-ui/core/TableFooter";
import TableRow from "@material-ui/core/TableRow";
import Dataset from "./Dataset/Dataset";


class GetFabricated extends Component {

    state = {
        fabricatedDatasetGroups: [],
        showPairs: {},
        page: 0,
        rowsPerPage: 5,
        loading: false
    }

    componentDidMount() {
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/get_fabricated_dataset_groups"
        }).then(res => {
            const showPairs = {}
            res.data.forEach(datasetGroup => showPairs[datasetGroup] = false);
            this.setState({fabricatedDatasetGroups: res.data, showPairs: showPairs});
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

    downloadDataset = (datasetId) => {
        this.setState({loading: true});
        axios({
            method: "get",
            url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/download_fabricated_dataset/" + datasetId,
            responseType: 'blob',
        }).then(res => {
            const url = window.URL.createObjectURL(new Blob([res.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', datasetId + '.zip');
            document.body.appendChild(link);
            link.click();
            this.setState({loading: false});
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    showSample = (datasetId) => {
        const fabricatedData = {...this.state.fabricatedData};
        const dataset = fabricatedData[datasetId];
        if(dataset.showSample){
            dataset.showSample = false;
            this.setState({fabricatedData: fabricatedData});
        }else if(Object.keys(dataset.sample).length !== 0){
            dataset.showSample = true;
            this.setState({fabricatedData: fabricatedData});
        }else{
            axios({
                 method: "get",
                 url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/get_fabricated_sample/" + datasetId
            }).then(res => {
                dataset.showSample = true;
                dataset.sample = res.data;
                this.setState({fabricatedData: fabricatedData});
            }).catch(err => {
                console.log(err);
            })
        }
    }

    showPairs = (datasetId) => {
        const showPairs = {...this.state.showPairs}
        showPairs[datasetId] = !showPairs[datasetId]
        this.setState({showPairs: showPairs})
    }


    render() {
        return (
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <div className={classes.Parent}>
                    <TableContainer className={classes.Container}>
                        <Table className={classes.Results}>
                            <TableBody>
                                {this.state.fabricatedDatasetGroups.slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((datasetId) => {
                                        const datasetGroupPairs = this.state.showPairs[datasetId]
                                            ? <Dataset key={datasetId}
                                                       datasetId={datasetId}/>
                                            : null;
                                        return (<div className={classes.Result}>
                                            <p className={classes.Paragraph}>Dataset group: {datasetId}</p>
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
                                                    backgroundColor: "#016b9f",
                                                    color: "white",
                                                    padding: "10px 10px",
                                                    fontSize: "11px"
                                                }}
                                                onClick={() => this.showPairs(datasetId)}>
                                                Show pairs
                                            </Button>
                                            {datasetGroupPairs}
                                        </div>);
                                    })
                                }
                            </TableBody>
                            <TableFooter>
                                <TableRow>
                                    <div className={classes.Pagination}>
                                    <TablePagination
                                        rowsPerPageOptions={[5, 10, 25]}
                                        component="div"
                                        count={this.state.fabricatedDatasetGroups.length}
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

export default GetFabricated;
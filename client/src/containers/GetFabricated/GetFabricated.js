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
        fabricatedData: {
            "miller":  {datasetIds: ["miller_both_0_1_ac1_av", "miller_both_50_70_ac4_av", "miller_both_0_1_ac1_ev",
                    "miller_both_50_70_ac4_ev", "miller_both_0_1_ac2_av", "miller_both_50_70_ac5_av", "miller_both_0_1_ac2_ev",
                    "miller_both_50_70_ac5_ev", "miller_both_0_1_ac3_av", "miller_both_50_70_ec_av", "miller_both_0_1_ac3_ev",
                    "miller_both_50_70_ec_ev", "miller_both_0_1_ac4_av", "miller_horizontal_0_ac1_av", "miller_both_0_1_ac4_ev",
                    "miller_horizontal_0_ac1_ev", "miller_both_0_1_ac5_av", "miller_horizontal_0_ac2_av", "miller_both_0_1_ac5_ev",
                    "miller_horizontal_0_ac2_ev", "miller_both_0_1_ec_av", "miller_horizontal_0_ac3_av", "miller_both_0_1_ec_ev",
                    "miller_horizontal_0_ac3_ev", "miller_both_0_30_ac1_av", "miller_horizontal_0_ac4_av", "miller_both_0_30_ac1_ev",
                    "miller_horizontal_0_ac4_ev", "miller_both_0_30_ac2_av", "miller_horizontal_0_ac5_av", "miller_both_0_30_ac2_ev",
                    "miller_horizontal_0_ac5_ev", "miller_both_0_30_ac3_av", "miller_horizontal_0_ec_av", "miller_both_0_30_ac3_ev",
                    "miller_horizontal_0_ec_ev", "miller_both_0_30_ac4_av", "miller_horizontal_100_ac1_av", "miller_both_0_30_ac4_ev",
                    "miller_horizontal_100_ac1_ev"]},
        },
        page: 0,
        rowsPerPage: 5,
        loading: false
    }

    componentDidMount() {
        // this.setState({loading: true})
        // axios({
        //      method: "get",
        //      url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/get_fabricated_data"
        // }).then(res => {
        //     let fabricatedData = {};
        //     Object.keys(res.data).forEach((fabricatedDataId) => fabricatedData[fabricatedDataId] =
        //         {datasetIds: res.data[fabricatedDataId], showSample: false, sample: {}});
        //     this.setState({loading: false, fabricatedData: fabricatedData});
        // }).catch(err => {
        //     this.setState({loading: false});
        //     console.log(err);
        // })
    }


    handleChangePage = (event, newPage) => {
        this.setState({page: newPage});
    };

    handleChangeRowsPerPage = (event) => {
        this.setState({rowsPerPage: +event.target.value});
        this.setState({page: 0});
    };

    deleteDataset = (datasetId) => {
        this.setState({loading: true});
        axios({
             method: "post",
             url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/delete_fabricated_dataset/" + datasetId
        }).then(() => {
            const fabricatedData = {...this.state.fabricatedData};
            delete fabricatedData[datasetId];
            this.setState({fabricatedData: fabricatedData, loading: false});
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

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
            this.setState({loading: true});
            axios({
                 method: "get",
                 url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/get_fabricated_sample/" + datasetId
            }).then(res => {
                dataset.showSample = true;
                dataset.sample = res.data;
                this.setState({loading: false, fabricatedData: fabricatedData});
            }).catch(err => {
                this.setState({loading: false});
                console.log(err);
            })
        }
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
                                {Object.keys(this.state.fabricatedData).slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((datasetId) => {
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
                                            <Dataset pairIds={this.state.fabricatedData[datasetId].datasetIds}/>
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
                                        count={Object.keys(this.state.fabricatedData).length}
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
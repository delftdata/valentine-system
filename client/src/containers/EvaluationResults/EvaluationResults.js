import React, {Component} from "react";
import Aux from "../../hoc/Aux";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import classes from "./EvaluationResults.module.css";
import {TableContainer} from "@material-ui/core";
import Table from "@material-ui/core/Table";
import TableFooter from '@material-ui/core/TableFooter';
import TableBody from "@material-ui/core/TableBody";
import TablePagination from "@material-ui/core/TablePagination";
import axios from "axios";
import EvaluationResult from "./EvaluationResult/EvaluationResult";
import TableRow from "@material-ui/core/TableRow";
import TestFigure from "../../assets/Unionable-all-1.png";
import Button from "@material-ui/core/Button";
import BarChartIcon from "@material-ui/icons/BarChart";
import GetAppIcon from "@material-ui/icons/GetApp";


class EvaluationResults extends Component {

    state = {
        evaluationResults: {"miller": ["miller_both_0_1_ac1_av", "miller_both_50_70_ac4_av", "miller_both_0_1_ac1_ev",
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
        page: 0,
        rowsPerPage: 5,
        loading: false,
        showPlot: false,
    }

    componentDidMount() {
        this.setState({loading: true})
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/get_evaluation_results"
        }).then(res => {
            this.setState({loading: false, evaluationResults: res.data});
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    displayBoxplot = (fabricatedPairId) => {
        this.setState({showPlot: true});
    }

    closeShowDataHandler = () => {
        this.setState({showPlot: false});
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
        return (
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <Modal show={this.state.showPlot} modalClosed={this.closeShowDataHandler} figure={true}>
                    <img src={TestFigure} alt={"figure"} className={classes.Modal}/>
                </Modal>
                <div className={classes.Parent}>
                    <TableContainer className={classes.Container}>
                        <Table className={classes.Results}>
                            <TableBody>
                                {Object.keys(this.state.evaluationResults).slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((datasetId) => {
                                        return (<div className={classes.Result}>
                                            <p className={classes.Paragraph}>Job: 264c73fe-53a8-43b7-a837-c3d0c6b7373a</p>
                                            <p className={classes.Paragraph}>Dataset group: {datasetId}</p>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    color: "#016b9f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px",
                                                    marginRight: "10px"
                                                }}
                                                onClick={() => this.displayBoxplot(datasetId)}>
                                                <BarChartIcon/>
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
                                            <EvaluationResult pairIds={this.state.evaluationResults[datasetId]}/>
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
                                        count={Object.keys(this.state.evaluationResults).length}
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

export default EvaluationResults;
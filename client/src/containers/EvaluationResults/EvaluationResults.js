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
import Button from "@material-ui/core/Button";
import BarChartIcon from "@material-ui/icons/BarChart";
import GetAppIcon from "@material-ui/icons/GetApp";


const FileSaver = require('file-saver');

class EvaluationResults extends Component {

    state = {
        evaluationResults: [],
        showPairs: {},
        boxplotData: '',
        page: 0,
        rowsPerPage: 5,
        loading: false,
        showPlot: false,
    }

    componentDidMount() {
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/get_evaluation_results"
        }).then(res => {
            const showPairs = {}
            for (let job in res.data){
                showPairs[job['job_id']] = false
            }
            this.setState({evaluationResults: res.data, showPairs: showPairs});
        }).catch(err => {
            console.log(err);
        })
    }

    displayBoxplot = (fabricatedPairId, datasetGroup) => {
        this.setState({loading: true});
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/download_boxplots/" +
                 fabricatedPairId +
                 "__dataset_group__" +
                 datasetGroup
        }).then(res => {
            this.setState({boxplotData: res.data['result'], showPlot: true, loading: false});
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    closeShowDataHandler = () => {
        this.setState({showPlot: false});
    }

    downloadDataset = (jobId, datasetGroup) => {
        this.setState({loading: true});
        axios({
            method: "get",
            url: process.env.REACT_APP_SERVER_ADDRESS +
                "/valentine/results/download_evaluation_results/" +
                jobId +
                "/"+
                datasetGroup,
            responseType: 'blob',
        }).then(res => {
            this.setState({loading: false});
            FileSaver.saveAs(res.data, jobId + '.zip');
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    showPairs = (jobId) => {
        const showPairs = {...this.state.showPairs}
        showPairs[jobId] = !showPairs[jobId]
        this.setState({showPairs: showPairs})
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
                    <img src={`data:image/png;base64,${this.state.boxplotData}`}
                         alt={"figure"}
                         className={classes.Modal}/>
                </Modal>
                <div className={classes.Parent}>
                    <TableContainer className={classes.Container}>
                        <Table className={classes.Results}>
                            <TableBody>
                                {this.state.evaluationResults.slice(this.state.page * this.state.rowsPerPage,
                                    this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                    .map((evaluationResult) => {
                                        const datasetGroupPairEvaluationResults =
                                            this.state.showPairs[evaluationResult['job_id']]
                                            ?  <EvaluationResult
                                                    jobId={evaluationResult['job_id']}
                                                    groupId={evaluationResult['dataset_group']}/>
                                            : null;
                                        return (<div className={classes.Result}>
                                            <p className={classes.Paragraph}>
                                                Job: {evaluationResult['job_id']}
                                            </p>
                                            <p className={classes.Paragraph}>
                                                Dataset group: {evaluationResult['dataset_group']}
                                            </p>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    color: "#016b9f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px",
                                                    marginRight: "10px"
                                                }}
                                                onClick={() => this.displayBoxplot(evaluationResult['job_id'], evaluationResult['dataset_group'])}>
                                                <BarChartIcon/>
                                            </Button>
                                            <Button
                                                style={{
                                                    borderRadius: 10,
                                                    color: "#016b9f",
                                                    padding: "10px 10px",
                                                    fontSize: "8px"
                                                }}
                                                onClick={() => this.downloadDataset(
                                                    evaluationResult['job_id'],
                                                    evaluationResult['dataset_group'])}>
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
                                                onClick={() => this.showPairs(evaluationResult['job_id'])}>
                                                Show pairs
                                            </Button>
                                            {datasetGroupPairEvaluationResults}
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
                                        count={this.state.evaluationResults.length}
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
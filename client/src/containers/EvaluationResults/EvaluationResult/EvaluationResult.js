import React, {Component} from "react";
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
import axios from "axios";


const spurious_head = ["Column 1", "Column 2", "Similarity", "Type"]


class EvaluationResult extends Component {

    state = {
        loading: false,
        page: 0,
        rowsPerPage: 5,
        showSpurious: {},
        spurious: {},
        pairs: []
    }

    componentDidMount() {
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/get_evaluation_dataset_pairs/" +
                 this.props.jobId +
                 "/" +
                 this.props.groupId
        }).then(res => {
            const spurious = {};
            const showSpurious = {};
            for (let key in Object.keys(res.data)){
                spurious[res.data[key]["pair_name"]] = [];
                showSpurious[res.data[key]["pair_name"]] = false;
            }
            this.setState({pairs: res.data, showSpurious: showSpurious, spurious: spurious});
        }).catch(err => {
            console.log(err);
        })
    }

    downloadDataset = (fabricatedPairId, algorithm) => {
        axios({
            method: "get",
            url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/download_pairs_evaluation_result/" +
                 this.props.jobId +
                 "/" +
                 this.props.groupId +
                 "/" +
                 algorithm +
                 "/" +
                 fabricatedPairId,
            responseType: 'blob',
        }).then(res => {
            const url = window.URL.createObjectURL(new Blob([res.data]));
            const link = document.createElement('a');
            link.href = url;
            link.setAttribute('download', fabricatedPairId);
            document.body.appendChild(link);
            link.click();
        }).catch(err => {
            console.log(err);
        })
    }

    showSpuriousResults = (fabricatedPairId, algorithm) => {
        const showSpurious = {...this.state.showSpurious};
        const spurious = {...this.state.spurious};
        showSpurious[fabricatedPairId] = !showSpurious[fabricatedPairId];
        if (spurious[fabricatedPairId].length === 0){
            axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/get_evaluation_dataset_pair_spurious_results/" +
                 this.props.jobId +
                 "/" +
                 this.props.groupId +
                 "/" +
                 algorithm +
                 "/" +
                 fabricatedPairId
            }).then(res => {
                spurious[fabricatedPairId] = res.data
                this.setState({spurious: spurious})
            }).catch(err => {
                console.log(err);
            })
        }
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
            <div className={classes.Parent}>
                <TableContainer className={classes.Container}>
                    <Table className={classes.Results}>
                        <TableBody>
                            {this.state.pairs.slice(this.state.page * this.state.rowsPerPage,
                                this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                .map((pair) => {
                                    const spuriousData = this.state.spurious[pair["pair_name"]].length === 0 ?
                                        <p>No spurious data, perfect result</p>
                                        : <SimpleTable
                                                    head={spurious_head}
                                                    body={this.state.spurious[pair["pair_name"]]}/>;
                                    const spuriousResults = this.state.showSpurious[pair["pair_name"]] ?
                                            <div>
                                                {spuriousData}
                                            </div>
                                            : null;
                                    return (<div className={classes.FabricatedPair}>
                                                <p>Fabricated pair: {pair["pair_name"]}</p>
                                                <p>Algorithm: {pair["algorithm"]}</p>
                                                <Button
                                                    style={{
                                                        borderRadius: 10,
                                                        color: "#016b9f",
                                                        padding: "10px 10px",
                                                        fontSize: "8px"
                                                    }}
                                                    onClick={() => this.downloadDataset(
                                                        pair["pair_name"],
                                                        pair["algorithm"])}>
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
                                                    onClick={() => this.showSpuriousResults(pair["pair_name"],
                                                        pair["algorithm"])}>
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
                                    count={this.state.pairs.length}
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
        );
    }

}

export default EvaluationResult;
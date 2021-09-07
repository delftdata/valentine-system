import React, {Component} from "react";
import Button from "@material-ui/core/Button";
import axios from "axios";

import classes from "./MatchList.module.css";
import Modal from "../../../components/UI/Modal/Modal";
import Spinner from "../../../components/UI/Spinner/Spinner";
import Aux from "../../../hoc/Aux";
import Paper from "@material-ui/core/Paper";
import TableContainer from "@material-ui/core/TableContainer";
import TablePagination from "@material-ui/core/TablePagination";
import TableBody from "@material-ui/core/TableBody";
import Table from "@material-ui/core/Table";
import TableRow from "@material-ui/core/TableRow";
import TableHead from "@material-ui/core/TableHead";
import TableCell from "@material-ui/core/TableCell";
import {withStyles} from "@material-ui/core/styles";
import ColumnPreview from "./ColumnPreview/ColumnPreview";
import GradientProgressBar from "../../../components/UI/ProgressBar/GradientProgressBar";


const StyledTableCell = withStyles((theme) => ({
  head: {
      backgroundColor: "#534bae",
      border: 1,
      borderRadius: 0,
      boxShadow: "1px 1px 1px 1px rgba(0, 0, 0, 1)",
      color: theme.palette.common.white,
      fontWeight: "bold",
  },
  body: {
    fontSize: 15,
  },
}))(TableCell);


const StyledTableRow = withStyles((theme) => ({
  root: {
    "&:nth-of-type(odd)": {
      backgroundColor: theme.palette.action.hover,
    },
  },
}))(TableRow);


class MatchList extends Component {

    state = {
        page: 0,
        rowsPerPage: 10,
        rankedList: [],
        jobId: "",
        loading: false,
        showData: false,
        targetColumn: "",
        sourceColumn: "",
        sourceData:[],
        targetData: []
    }

    componentDidMount() {
        this.setState({rankedList: this.props.rankedList, jobId: this.props.jobId});
    }

    deleteMatchHandler = (matchIndex, save) => {
        const rankedList = [...this.state.rankedList];
        rankedList.splice(matchIndex, 1);
        this.setState({rankedList: rankedList});
        if(save){
            axios({
                 method: "post",
                 url: process.env.REACT_APP_SERVER_ADDRESS + "/results/save_verified_match/" + this.state.jobId + "/" + matchIndex
            }).then(() => {
                console.log('save_verified_match responded');
            }).catch(err => {
                console.log(err);
            });
        }else{
            axios({
                 method: "post",
                 url: process.env.REACT_APP_SERVER_ADDRESS + "/results/discard_match/" + this.state.jobId + "/" + matchIndex
            }).then(() => {
                console.log('discard_match responded');
            }).catch(err => {
                console.log(err);
            })
        }
    }

    handleChangePage = (event, newPage) => {
        this.setState({page: newPage});
    };

    handleChangeRowsPerPage = (event) => {
        this.setState({rowsPerPage: +event.target.value});
        this.setState({page: 0});
    };

    closeShowDataHandler = () => {
        this.setState({showData: false});
    }

    getColumnSamples = (dbName, tableName, columnName, source) => {
        let source_db;
        if(source){
            source_db = this.props.sources['source']
        }else{
            source_db = this.props.sources['target']
        }
        axios({
                 method: "get",
                 url: process.env.REACT_APP_SERVER_ADDRESS +
                     "/matches/" + source_db + "/column_sample/" +
                     dbName +
                     "/" +
                     tableName +
                     "/" +
                     columnName
            }).then(res => {
                if(source){
                    this.setState({sourceData: res.data});
                }
                else{
                    this.setState({targetData: res.data});
                }
            }).catch(err => {
                console.log(err);
            })
    }

    showData = (sourceDbName, sourceTableName, sourceColumnName, targetDbName, targetTableName, targetColumnName) => {
        this.getColumnSamples(sourceDbName, sourceTableName, sourceColumnName, true);
        this.getColumnSamples(targetDbName, targetTableName, targetColumnName, false);
        this.setState({showData: true, targetColumn: targetColumnName, sourceColumn: sourceColumnName});
    }

    render() {
        return (
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <Modal show={this.state.showData} modalClosed={this.closeShowDataHandler}>
                    <ColumnPreview sourceName={this.state.sourceColumn} targetName={this.state.targetColumn}
                                   sourceData={this.state.sourceData} targetData={this.state.targetData}/>
                </Modal>
                <Paper>
                    <TableContainer className={classes.Container}>
                        <Table className={classes.List} size="small">
                            <TableHead>
                                <TableRow>
                                    <StyledTableCell align="center">
                                      Source Column
                                    </StyledTableCell>
                                    <StyledTableCell align="center">
                                      Target Column
                                    </StyledTableCell>
                                    <StyledTableCell align="center">
                                      Similarity
                                    </StyledTableCell>
                              </TableRow>
                            </TableHead>
                            <TableBody>
                            {this.state.rankedList
                                .slice(this.state.page * this.state.rowsPerPage, this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                                .map((item, index) => (
                                    <StyledTableRow key={index}>
                                        <StyledTableCell className={classes.Cell} align="center" onClick={() =>this.showData(item.source["db_guid"], item.source["tbl_nm"], item.source["clm_nm"], item.target["db_guid"], item.target["tbl_nm"], item.target["clm_nm"])}>
                                            {item.target["tbl_nm"]}.{item.target["clm_nm"]}
                                        </StyledTableCell>
                                        <StyledTableCell className={classes.Cell} align="center" onClick={() =>this.showData(item.source["db_guid"], item.source["tbl_nm"], item.source["clm_nm"], item.target["db_guid"], item.target["tbl_nm"], item.target["clm_nm"])}>
                                            {item.source["tbl_nm"]}.{item.source["clm_nm"]}
                                        </StyledTableCell>
                                        <TableCell align="center" onClick={() =>this.showData(item.source["db_guid"], item.source["tbl_nm"], item.source["clm_nm"], item.target["db_guid"], item.target["tbl_nm"], item.target["clm_nm"])}>
                                            <GradientProgressBar similarity={item["sim"]} />
                                        </TableCell>
                                        <StyledTableCell className={classes.Cell} align="left">
                                            <Button variant="contained"
                                                    style={{
                                                        borderRadius: 10,
                                                        color: "white",
                                                        background: "#016b9f",
                                                        padding: "10px 10px",
                                                        fontSize: "14px"
                                                    }}
                                                    onClick={() => this.deleteMatchHandler(index, true)}>
                                                Verify
                                            </Button>
                                            <Button style={{
                                                        borderRadius: 10,
                                                        color: "white",
                                                        padding: "10px 10px",
                                                        marginLeft: "10px",
                                                        fontSize: "14px",
                                                        background: "#71100f"
                                                    }}
                                                    onClick={() => this.deleteMatchHandler(index, false)}>
                                                Discard
                                            </Button>
                                        </StyledTableCell>
                                    </StyledTableRow>
                                )
                            )}
                            </TableBody>
                        </Table>
                    </TableContainer>
                    <TablePagination
                    rowsPerPageOptions={[10, 25, 100]}
                    component="div"
                    count={this.state.rankedList.length}
                    rowsPerPage={this.state.rowsPerPage}
                    page={this.state.page}
                    onChangePage={this.handleChangePage}
                    onChangeRowsPerPage={this.handleChangeRowsPerPage}
                    />
                </Paper>
            </Aux>
        );
    }
}

export default MatchList;
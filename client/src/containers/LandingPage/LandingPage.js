import React, { Component } from "react";
import Aux from "../../hoc/Aux";
import { withRouter } from 'react-router-dom'
import researchLogo from "../../assets/research-svgrepo-com.svg";
import dataLakeLogo from "../../assets/data-lake.svg"
import delftKiss from "../../assets/delft_kiss.png"
import classes from "./LandingPage.module.css";
import TUDLogo from "../../assets/TU_P5_white.png";
import {Link} from "@material-ui/core";


const ResearchButton = withRouter(({ history }) => (
    <div className={classes.LeftLandingPageItem} onClick={() => { history.push('/dataset_fabrication') }}>
        <h3>Schema Matching Evaluation & Experimentation</h3>
        <img src={researchLogo} alt={"Research logo"} width="500px" height="200px"/>
        <p>Evaluate and compare schema matching methods</p>
    </div>
));


const DataLakeButton = withRouter(({ history }) => (
    <div className={classes.RightLandingPageItem} onClick={() => { history.push('/matcher') }}>
        <h3>Holistic Matching in a Data Lake</h3>
        <img src={dataLakeLogo} alt={"Data lake logo"} width="500px" height="200px"/>
        <p>Capture relevance among datasets in a data lake</p>
    </div>
));


class LandingPage extends Component {

    render(){
        return(
            <Aux>
                <h1 className={classes.LandingPageTitle}>Valentine</h1>
                <div className={classes.Kiss}>
                    <img src={delftKiss} alt={"Delft kiss"} width="300px" height="192px"/>
                </div>
                <div className={classes.LandingPageItems}>
                    <ResearchButton/>
                    <DataLakeButton/>
                </div>
                <div className={classes.Footer}>
                    <img src={TUDLogo} alt={"TU Delft logo"}/>
                    <div className={classes.Link}>
                        <Link href="https://github.com/delftdata" onClick={this.preventDefault} color="#263238">
                            TU Delft Data Management Team
                        </Link>
                    </div>
                </div>
            </Aux>
        );
    }

}

export default LandingPage;
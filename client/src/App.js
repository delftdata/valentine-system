import React, { Component } from "react";
import { Route, Switch } from "react-router-dom";
import axios from "axios";
import Layout from "./components/Layout/Layout";
import Matcher from "./containers/Matcher/Matcher";
import AlgorithmEvaluation from "./containers/AlgorithmEvaluation/AlgorithmEvaluation"
import DatasetFabrication from "./containers/DatasetFabrication/DatasetFabrication"
import EvaluationResults from "./containers/EvaluationResults/EvaluationResults"
import GetFabricated from "./containers/GetFabricated/GetFabricated"
import Results from "./containers/Results/Results"
import VerifiedMatches from "./containers/VerifiedMatches/VerifiedMatches";
import LandingPage from "./containers/LandingPage/LandingPage";
import NoMatch from "./components/UI/NoMatch/NoMatch"
import BackgroundImage from "./assets/background.jpg"
import AccountTreeTwoToneIcon from '@material-ui/icons/AccountTreeTwoTone';
import AssignmentTwoToneIcon from '@material-ui/icons/AssignmentTwoTone';
import AssignmentTurnedInTwoToneIcon from '@material-ui/icons/AssignmentTurnedInTwoTone';
import BlurLinearTwoToneIcon from '@material-ui/icons/BlurLinearTwoTone';
import BuildTwoToneIcon from '@material-ui/icons/BuildTwoTone';
import ZoomInTwoToneIcon from '@material-ui/icons/ZoomInTwoTone';
import AssessmentTwoToneIcon from '@material-ui/icons/AssessmentTwoTone';

axios.defaults.maxBodyLength = Infinity
axios.defaults.maxContentLength = Infinity

class App extends Component {

    render() {

        const dataLakeToolbarElements = [
            {
                link: "/matcher",
                text: "Matcher",
                icon: <AccountTreeTwoToneIcon/>
            },
            {
                link: "/results",
                text: "Results",
                icon: <AssignmentTwoToneIcon/>
            },
            {
                link: "/verified_matches",
                text: "Verified Matches",
                icon: <AssignmentTurnedInTwoToneIcon/>
            }
        ]

        const researchToolbarElements = [
            {
                link: "/dataset_fabrication",
                text: "Dataset Fabrication",
                icon: <BlurLinearTwoToneIcon/>
            },
            {
                link: "/get_fabricated",
                text: "Inspect Datasets",
                icon: <ZoomInTwoToneIcon/>
            },
            {
                link: "/algorithm_evaluation",
                text: "Experiment Configuration",
                icon: <BuildTwoToneIcon/>
            },
            {
                link: "/evaluation_results",
                text: "Evaluation Results",
                icon: <AssessmentTwoToneIcon/>
            }
        ]

        return (
          <div style={{
              position: 'fixed',
              top: '0',
              left: '0',
              minWidth: '100%',
              minHeight: '100%',
              backgroundImage: "url(" + BackgroundImage + ")",
              backgroundPosition: 'center',
              backgroundSize: 'cover',
              backgroundRepeat: 'no-repeat'}}>
              <Switch>
                  <Route exact path="/">
                      <LandingPage/>
                  </Route>
                  <Route path="/verified_matches">
                      <Layout toolbar_elements={dataLakeToolbarElements}>
                          <VerifiedMatches/>
                      </Layout>
                  </Route>
                  <Route path="/results">
                      <Layout toolbar_elements={dataLakeToolbarElements}>
                          <Results/>
                      </Layout>
                  </Route>
                  <Route path="/matcher">
                      <Layout toolbar_elements={dataLakeToolbarElements}>
                          <Matcher/>
                      </Layout>
                  </Route>
                  <Route path="/dataset_fabrication">
                      <Layout toolbar_elements={researchToolbarElements}>
                          <DatasetFabrication/>
                      </Layout>
                  </Route>
                  <Route path="/get_fabricated">
                      <Layout toolbar_elements={researchToolbarElements}>
                          <GetFabricated/>
                      </Layout>
                  </Route>
                  <Route path="/algorithm_evaluation">
                      <Layout toolbar_elements={researchToolbarElements}>
                          <AlgorithmEvaluation/>
                      </Layout>
                  </Route>
                  <Route path="/evaluation_results">
                      <Layout toolbar_elements={researchToolbarElements}>
                          <EvaluationResults/>
                      </Layout>
                  </Route>
                  <Route>
                      <NoMatch/>
                  </Route>
              </Switch>
          </div>
        );
      }
}

export default App;

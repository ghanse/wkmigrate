import Layout from '@theme/Layout';
import Footer from '@theme/Footer';
import { JSX } from 'react';
import Button from '../components/Button';
import { Info, FileText, Activity, AlertTriangle, CheckCircle, Grid, BarChart2, Code, PieChart } from 'lucide-react';

const Hero = () => {
  return (

    <div className="px-4 md:px-10 pt-16 pb-8 flex flex-col justify-center items-center w-full">

      <h1 className="text-4xl md:text-5xl font-semibold text-center mb-6">
        wkmigrate - Automated workflow migration
      </h1>
      <p className="text-lg text-center text-balance">
        Programmatically translate Azure Data Factory pipelines to Databricks jobs.
      </p>

      {/* Call to Action Buttons */}
      <div className="mt-12 flex flex-col space-y-4 md:flex-row md:space-y-0 md:space-x-4">
      <Button
          variant="secondary"
          outline={true}
          link="/docs/motivation"
          size="large"
          label={"Motivation"}
          className="w-64"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/guide"
          size="large"
          label={"Usage Guide"}
          className="w-64"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/capabilities"
          size="large"
          label={"Capabilities"}
          className="w-64"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/reference"
          size="large"
          label={"Reference"}
          className="w-64"
        />
      </div>
    </div>
  );
};


export default function Home(): JSX.Element {
  return (
    <Layout noFooter>
      <main
        className="flex flex-col"
        style={{ minHeight: 'calc(100vh - var(--ifm-navbar-height))' }}
      >
        <div className="flex justify-center items-center mx-auto flex-1">
          <div className="max-w-screen-lg">
            <Hero />
          </div>
        </div>
        <Footer />
      </main>
    </Layout>
  );
}

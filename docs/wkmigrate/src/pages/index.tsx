import Layout from '@theme/Layout';
import { JSX } from 'react';
import Button from '../components/Button';
import { Info, FileText, Activity, AlertTriangle, CheckCircle, Grid, BarChart2, Code, PieChart } from 'lucide-react';

const CallToAction = () => {
  return (
    <div className="flex flex-col items-center py-12">
      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Migrate your data pipelines today 🚀
      </h2>
      <p className="text-center mb-6 text-pretty">
        Follow our examples to get started with wkmigrate.
      </p>
      <Button
        variant="primary"
        link="https://github.com/ghanse/wkmigrate/tree/main/examples"
        size="large"
        label="Start using wkmigrate ✨"
        className="w-full md:w-auto p-4 font-mono bg-gradient-to-r from-blue-500 to-purple-500 text-white hover:from-blue-600 hover:to-purple-600 transition-all duration-300"
      />
    </div>
  )
};

const Hero = () => {
  return (

    <div className="px-4 md:px-10 pt-16 pb-8 flex flex-col justify-center items-center w-full">

      <h1 className="text-4xl md:text-5xl font-semibold text-center mb-6">
        wkmigrate - Automated workflow migration
      </h1>
      <p className="text-lg text-center text-balance">
        The wkmigrate library allows you to programmatically list and translate pipelines in ADF to equivalent jobs in Databricks.
      </p>

      {/* Call to Action Buttons */}
      <div className="mt-12 flex flex-col space-y-4 md:flex-row md:space-y-0 md:space-x-4">
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
          link="/docs/reference/api"
          size="large"
          label={"API Reference"}
          className="w-64"
        />
      </div>
    </div>
  );
};


export default function Home(): JSX.Element {
  return (
    <Layout>
      <main>
        <div className='flex justify-center items-center min-h-screen mx-auto'>
          <div className='max-w-screen-lg'>
            <Hero />
            <CallToAction />
          </div>
        </div>
      </main>
    </Layout>
  );
}

using System.Text;
using DLinq.Extensions;
using DLinq.Sources;
using SkiaSharp;
using TorchSharp;

namespace DLinq.Exemples
{
    public class ObjectDetection
    {
        public static void Run(MPI.Intracommunicator comm)
        {
            var stream = FileSource.ReadFile(comm, "Exemples/ObjectDetection/imagelist.txt", Encoding.UTF8, batchSize: 1);
            stream.Transformation((input) =>
            {
                var device = torch.cuda.is_available() ? torch.CUDA : torch.CPU;
                var categories = File.ReadAllLines("Exemples/ObjectDetection/imagenet_classes.txt");
                var model = torchvision.models.mobilenet_v2(num_classes: categories.Length);
                model.eval();
                model.to(device);

                var preprocess = torchvision.transforms.Compose(
                    torchvision.transforms.Resize(256),
                    torchvision.transforms.CenterCrop(224),
                    torchvision.transforms.Normalize(means: new double[] { 0.485, 0.456, 0.406 }, stdevs: new double[] { 0.229, 0.224, 0.225 })
                );

                var results = new List<Tuple<string, double>>();
                var image_tensors = LoadImages(input.Data, 4, 3, 256, 256);
                foreach (var tensor in image_tensors)
                {
                    var input_tensor = preprocess.call(tensor).to(device);
                    var output = model.call(input_tensor);
                    var probabilities = torch.nn.functional.softmax(output[0], dim: 0);
                    var (topProb, topId) = torch.topk(probabilities, 1);
                    var label = categories[topId[0].ToInt32()];
                    var accuracy = topProb[0].ToDouble();
                    results.Add(Tuple.Create(label, accuracy));
                }

                return results;
            })
            .Sink((input) =>
            {
                foreach (var item in input.Data)
                {
                    Console.WriteLine($"{item.Item1}: {item.Item2}");
                }
            });
        }

        private static List<torch.Tensor> LoadImages(IList<string> images, int batchSize, int channels, int height, int width)
        {
            List<torch.Tensor> tensors = new List<torch.Tensor>();

            var imgSize = channels * height * width;
            bool shuffle = false;

            Random rnd = new Random();
            var indices = !shuffle ?
                Enumerable.Range(0, images.Count).ToArray() :
                Enumerable.Range(0, images.Count).OrderBy(c => rnd.Next()).ToArray();


            // Go through the data and create tensors
            for (var i = 0; i < images.Count;)
            {

                var take = Math.Min(batchSize, Math.Max(0, images.Count - i));

                if (take < 1) break;

                var dataTensor = torch.zeros(new long[] { take, imgSize }, torch.ScalarType.Byte);

                // Take
                for (var j = 0; j < take; j++)
                {
                    var idx = indices[i++];
                    var lblStart = idx * (1 + imgSize);
                    var imgStart = lblStart + 1;

                    using (var stream = new SKManagedStream(File.OpenRead(images[idx])))
                    using (var bitmap = SKBitmap.Decode(stream))
                    {
                        using (var inputTensor = torch.tensor(GetBytesWithoutAlpha(bitmap)))
                        {

                            var finalized = inputTensor;

                            var nz = inputTensor.count_nonzero().item<long>();

                            if (bitmap.Width != width || bitmap.Height != height)
                            {
                                var t = inputTensor.reshape(1, channels, bitmap.Height, bitmap.Width);
                                finalized = torchvision.transforms.functional.resize(t, height, width).reshape(imgSize);
                            }

                            dataTensor.index_put_(finalized, torch.TensorIndex.Single(j));
                        }
                    }
                }

                tensors.Add(dataTensor.reshape(take, channels, height, width));
                dataTensor.Dispose();
            }

            return tensors;
        }

        private static byte[] GetBytesWithoutAlpha(SKBitmap bitmap)
        {
            var height = bitmap.Height;
            var width = bitmap.Width;

            var inputBytes = bitmap.Bytes;

            if (bitmap.ColorType == SKColorType.Gray8)
                return inputBytes;

            if (bitmap.BytesPerPixel != 4 && bitmap.BytesPerPixel != 1)
                throw new ArgumentException("Conversion only supports grayscale and ARGB");

            var channelLength = height * width;

            var channelCount = 3;

            int inputBlue = 0, inputGreen = 0, inputRed = 0;
            int outputRed = 0, outputGreen = channelLength, outputBlue = channelLength * 2;

            switch (bitmap.ColorType)
            {
                case SKColorType.Bgra8888:
                    inputBlue = 0;
                    inputGreen = 1;
                    inputRed = 2;
                    break;

                default:
                    throw new NotImplementedException($"Conversion from {bitmap.ColorType} to bytes");
            }
            var outBytes = new byte[channelCount * channelLength];

            for (int i = 0, j = 0; i < channelLength; i += 1, j += 4)
            {
                outBytes[outputRed + i] = inputBytes[inputRed + j];
                outBytes[outputGreen + i] = inputBytes[inputGreen + j];
                outBytes[outputBlue + i] = inputBytes[inputBlue + j];
            }

            return outBytes;
        }
    }
}
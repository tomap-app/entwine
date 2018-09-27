/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/util/executor.hpp>

#include <sstream>

#include <pdal/Dimension.hpp>
#include <pdal/QuickInfo.hpp>
#include <pdal/SpatialReference.hpp>
#include <pdal/StageFactory.hpp>
#include <pdal/io/BufferReader.hpp>
#include <pdal/io/LasReader.hpp>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

namespace
{
    class BufferState
    {
    public:
        BufferState(const Bounds& bounds)
            : m_table()
            , m_view(makeUnique<pdal::PointView>(m_table))
            , m_buffer()
        {
            using DimId = pdal::Dimension::Id;

            auto layout(m_table.layout());
            layout->registerDim(DimId::X);
            layout->registerDim(DimId::Y);
            layout->registerDim(DimId::Z);
            layout->finalize();

            std::size_t i(0);

            auto insert([this, &i, &bounds](int x, int y, int z)
            {
                m_view->setField(DimId::X, i, bounds[x ? 0 : 3]);
                m_view->setField(DimId::Y, i, bounds[y ? 1 : 4]);
                m_view->setField(DimId::Z, i, bounds[z ? 2 : 5]);
                ++i;
            });

            insert(0, 0, 0);
            insert(0, 0, 1);
            insert(0, 1, 0);
            insert(0, 1, 1);
            insert(1, 0, 0);
            insert(1, 0, 1);
            insert(1, 1, 0);
            insert(1, 1, 1);

            m_buffer.addView(m_view);
        }

        pdal::PointTable& getTable() { return m_table; }
        pdal::PointView& getView() { return *m_view; }
        pdal::BufferReader& getBuffer() { return m_buffer; }

    private:
        pdal::PointTable m_table;
        pdal::PointViewPtr m_view;
        pdal::BufferReader m_buffer;
    };
}

Executor::Executor()
    : m_stageFactory(makeUnique<pdal::StageFactory>())
{ }

Executor::~Executor()
{ }

bool Executor::good(const std::string path) const
{
    auto ext(arbiter::Arbiter::getExtension(path));
    return !m_stageFactory->inferReaderDriver(path).empty();
}

std::vector<std::string> Executor::dims(const std::string path) const
{
    std::vector<std::string> list;
    UniqueStage scopedReader(createReader(path));
    pdal::Stage* reader(scopedReader->get());
    pdal::PointTable table;
    { auto lock(getLock()); reader->prepare(table); }
    for (const auto& id : table.layout()->dims())
    {
        list.push_back(table.layout()->dimName(id));
    }
    return list;
}

/*
UniqueStage Executor::createReader(const std::string path) const
{
    UniqueStage result;

    const std::string driver(m_stageFactory->inferReaderDriver(path));
    if (driver.empty()) return result;

    auto lock(getLock());

    if (pdal::Stage* reader = m_stageFactory->createStage(driver))
    {
        pdal::Options options;
        options.add(pdal::Option("filename", path));
        reader->setOptions(options);

        // Unlock before creating the ScopedStage, in case of a throw we can't
        // hold the lock during its destructor.
        lock.unlock();

        result.reset(new ScopedStage(reader, *m_stageFactory, mutex()));
    }

    return result;
}
*/

std::unique_ptr<Preview> Executor::preview(
        const std::string path,
        const Reprojection* reprojection)
{
    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return nullptr;

    auto pre(makeUnique<Preview>());

    pdal::Stage* reader(scopedReader->get());
    const pdal::QuickInfo qi(([this, reader]()
    {
        auto lock(getLock());
        return reader->preview();
    })());

    if (qi.valid())
    {
        if (!qi.m_bounds.empty())
        {
            pre->bounds = Bounds(
                    qi.m_bounds.minx, qi.m_bounds.miny, qi.m_bounds.minz,
                    qi.m_bounds.maxx, qi.m_bounds.maxy, qi.m_bounds.maxz);
        }

        { auto lock(getLock()); pre->srs = qi.m_srs.getWKT(); }
        pre->numPoints = qi.m_pointCount;
        pre->dimNames = qi.m_dimNames;

        if (const auto las = dynamic_cast<pdal::LasReader*>(reader))
        {
            const auto& h(las->header());
            pre->scale = makeUnique<Scale>(h.scaleX(), h.scaleY(), h.scaleZ());
        }

        pre->metadata = ([this, reader]()
        {
            auto lock(getLock());
            const auto s(pdal::Utils::toJSON(reader->getMetadata()));
            try { return parse(s); }
            catch (...) { return Json::Value(s); }
        })();
    }
    else
    {
        std::cout << "Deep scanning..." << std::endl;

        const Schema xyzSchema({ { DimId::X }, { DimId::Y }, { DimId::Z } });
        pre->bounds = Bounds::expander();

        VectorPointTable table(xyzSchema);
        table.setProcess([&pre, &table]()
        {
            // We'll pick up the number of points and bounds here.
            Point point;
            pre->numPoints += table.size();
            for (auto it(table.begin()); it != table.end(); ++it)
            {
                auto& pr(it.pointRef());
                point.x = pr.getFieldAs<double>(DimId::X);
                point.y = pr.getFieldAs<double>(DimId::Y);
                point.z = pr.getFieldAs<double>(DimId::Z);
                pre->bounds.grow(point);
            }
        });

        // TODO Reprojection.
        Json::Value pipeline;
        Json::Value reader;
        reader["filename"] = path;
        pipeline.append(reader);

        if (Executor::get().run(table, pipeline))
        {
            // And we'll pick up added dimensions here.
            for (const auto& d : xyzSchema.fixedLayout().added())
            {
                pre->dimNames.push_back(d.first);
            }
        }
    }

    if (!pre->numPoints) pre->bounds = Bounds();

    // Now we have all of our info in native format.  If a reprojection has
    // been set, then we'll need to transform our bounds and SRS values.
    if (pre->numPoints && reprojection)
    {
        using DimId = pdal::Dimension::Id;

        BufferState bufferState(pre->bounds);

        const auto selectedSrs(
                srsFoundOrDefault(qi.m_srs, *reprojection));

        UniqueStage scopedFilter(createReprojectionFilter(selectedSrs));
        if (!scopedFilter) return pre;

        pdal::Stage& filter(*scopedFilter->get());

        filter.setInput(bufferState.getBuffer());
        { auto lock(getLock()); filter.prepare(bufferState.getTable()); }
        filter.execute(bufferState.getTable());

        pre->bounds = Bounds::expander();
        for (std::size_t i(0); i < bufferState.getView().size(); ++i)
        {
            const Point point(
                    bufferState.getView().getFieldAs<double>(DimId::X, i),
                    bufferState.getView().getFieldAs<double>(DimId::Y, i),
                    bufferState.getView().getFieldAs<double>(DimId::Z, i));

            pre->bounds.grow(point);
        }

        auto lock(getLock());
        pre->srs = pdal::SpatialReference(reprojection->out()).getWKT();
    }

    return pre;
}


Bounds Executor::transform(
        const Bounds& bounds,
        const std::vector<double>& t) const
{
    BufferState bufferState(bounds);
    UniqueStage scopedFilter(createTransformationFilter(t));
    if (!scopedFilter)
    {
        throw std::runtime_error("Could not create transformation filter");
    }

    pdal::Stage& filter(*scopedFilter->get());

    filter.setInput(bufferState.getBuffer());
    { auto lock(getLock()); filter.prepare(bufferState.getTable()); }
    filter.execute(bufferState.getTable());

    Bounds b(Bounds::expander());

    using DimId = pdal::Dimension::Id;

    for (std::size_t i(0); i < bufferState.getView().size(); ++i)
    {
        const Point p(
                bufferState.getView().getFieldAs<double>(DimId::X, i),
                bufferState.getView().getFieldAs<double>(DimId::Y, i),
                bufferState.getView().getFieldAs<double>(DimId::Z, i));

        b.grow(p);
    }

    return b;
}

std::string Executor::getSrsString(const std::string input) const
{
    auto lock(getLock());
    return pdal::SpatialReference(input).getWKT();
}

bool Executor::run(pdal::StreamPointTable& table, const Json::Value& pipeline)
{
    std::istringstream iss(pipeline.toStyledString());

    auto lock(getLock());
    pdal::PipelineManager pm;
    pm.readPipeline(iss);

    if (pm.pipelineStreamable())
    {
        pm.validateStageOptions();
        pdal::Stage *s = pm.getStage();
        if (!s)
            return false;

        s->prepare(table);

        lock.unlock();
        s->execute(table);

        // pm.executeStream(table);
    }
    else
    {
        throw std::runtime_error("Only streaming for now...");
    }

    return true;
}

bool Executor::run(
        pdal::StreamPointTable& table,
        const std::string path,
        const Reprojection* reprojection,
        const std::vector<double>* transform,
        const std::vector<std::string> preserve)
{
    UniqueStage scopedReader(createReader(path));
    if (!scopedReader) return false;

    pdal::Stage* reader(scopedReader->get());
    pdal::Stage* executor(reader);

    // Needed so that the SRS has been initialized.
    { auto lock(getLock()); reader->prepare(table); }

    UniqueStage scopedFerry;

    if (preserve.size())
    {
        scopedFerry = createFerryFilter(preserve);
        if (!scopedFerry) return false;

        pdal::Stage* filter(scopedFerry->get());

        filter->setInput(*executor);
        executor = filter;
    }

    UniqueStage scopedReproj;

    if (reprojection)
    {
        const auto srs(
                srsFoundOrDefault(
                    reader->getSpatialReference(), *reprojection));

        scopedReproj = createReprojectionFilter(srs);
        if (!scopedReproj) return false;

        pdal::Stage* filter(scopedReproj->get());

        filter->setInput(*executor);
        executor = filter;
    }

    UniqueStage scopedTransform;

    if (transform)
    {
        scopedTransform = createTransformationFilter(*transform);
        if (!scopedTransform) return false;

        pdal::Stage* filter(scopedTransform->get());

        filter->setInput(*executor);
        executor = filter;
    }

    { auto lock(getLock()); executor->prepare(table); }

    try
    {
        executor->execute(table);
    }
    catch (pdal::pdal_error& e)
    {
        const std::string nostream("Point streaming not supported for stage");
        if (std::string(e.what()).find(nostream) == std::string::npos)
        {
            // If the error was from lack of streaming support, then we'll
            // fall back to the non-streaming API.  Otherwise, return false
            // indicating we couldn't successfully execute this file.
            return false;
        }

        static bool logged(false);
        if (!logged)
        {
            logged = true;
            std::cout <<
                "Streaming execution error - falling back to non-streaming: " <<
                e.what() << std::endl;
        }

        pdal::PointTable pdalTable;
        executor->prepare(pdalTable);
        auto views = executor->execute(pdalTable);

        pdal::PointView pooledView(table);
        auto& layout(*table.layout());
        const auto dimTypes(layout.dimTypes());

        std::vector<char> point(layout.pointSize(), 0);
        char* pos(point.data());

        std::size_t current(0);

        for (auto& view : views)
        {
            for (std::size_t i(0); i < view->size(); ++i)
            {
                view->getPackedPoint(dimTypes, i, pos);
                pooledView.setPackedPoint(dimTypes, current, pos);

                if (++current == table.capacity())
                {
                    table.reset();
                    current = 0;
                }
            }
        }

        if (current) table.reset();
    }

    return true;
}

UniqueStage Executor::createReader(const std::string path) const
{
    UniqueStage result;

    const std::string driver(m_stageFactory->inferReaderDriver(path));
    if (driver.empty()) return result;

    auto lock(getLock());

    if (pdal::Stage* reader = m_stageFactory->createStage(driver))
    {
        pdal::Options options;
        options.add(pdal::Option("filename", path));
        reader->setOptions(options);

        // Unlock before creating the ScopedStage, in case of a throw we can't
        // hold the lock during its destructor.
        lock.unlock();

        result.reset(new ScopedStage(reader, *m_stageFactory, mutex()));
    }

    return result;
}

UniqueStage Executor::createFerryFilter(const std::vector<std::string>& p) const
{
    UniqueStage result;

    if (p.empty()) throw std::runtime_error("No preservation option supplied");
    if (p.size() > 3) throw std::runtime_error("Too many preservation options");

    auto lock(getLock());

    if (pdal::Stage* filter = m_stageFactory->createStage("filters.ferry"))
    {
        std::size_t d(1);
        pdal::Options options;
        std::string s;
        for (const auto name : p)
        {
            s += (s.size() ? "," : "") +
                    pdal::Dimension::name(static_cast<pdal::Dimension::Id>(d)) +
                    "=" + name;
            ++d;
        }
        options.add("dimensions", s);
        filter->setOptions(options);

        lock.unlock();

        result.reset(new ScopedStage(filter, *m_stageFactory, mutex()));
    }

    return result;
}

UniqueStage Executor::createReprojectionFilter(const Reprojection& reproj) const
{
    UniqueStage result;

    if (reproj.in().empty())
    {
        throw std::runtime_error("No default SRS supplied, and none inferred");
    }

    auto lock(getLock());

    if (pdal::Stage* filter =
            m_stageFactory->createStage("filters.reprojection"))
    {
        pdal::Options options;
        options.add(pdal::Option("in_srs", reproj.in()));
        options.add(pdal::Option("out_srs", reproj.out()));
        filter->setOptions(options);

        // Unlock before creating the ScopedStage, in case of a throw we can't
        // hold the lock during its destructor.
        lock.unlock();

        result.reset(new ScopedStage(filter, *m_stageFactory, mutex()));
    }

    return result;
}

UniqueStage Executor::createTransformationFilter(
        const std::vector<double>& matrix) const
{
    UniqueStage result;

    if (matrix.size() != 16)
    {
        throw std::runtime_error(
                "Invalid matrix length " + std::to_string(matrix.size()));
    }

    auto lock(getLock());

    if (pdal::Stage* filter =
            m_stageFactory->createStage("filters.transformation"))
    {
        std::ostringstream ss;
        ss << std::setprecision(std::numeric_limits<double>::digits10);
        for (const double d : matrix) ss << d << " ";

        pdal::Options options;
        options.add(pdal::Option("matrix", ss.str()));
        filter->setOptions(options);

        lock.unlock();

        result = makeUnique<ScopedStage>(filter, *m_stageFactory, mutex());
    }

    return result;
}

std::unique_lock<std::mutex> Executor::getLock()
{
    return std::unique_lock<std::mutex>(mutex());
}

Reprojection Executor::srsFoundOrDefault(
        const pdal::SpatialReference& found,
        const Reprojection& given)
{
    if (given.hammer() || found.empty()) return given;
    else return Reprojection(found.getWKT(), given.out());
}

ScopedStage::ScopedStage(
        pdal::Stage* stage,
        pdal::StageFactory& stageFactory,
        std::mutex& factoryMutex)
    : m_stage(stage)
    , m_stageFactory(stageFactory)
    , m_factoryMutex(factoryMutex)
{ }

ScopedStage::~ScopedStage()
{
    std::lock_guard<std::mutex> lock(m_factoryMutex);
    m_stageFactory.destroyStage(m_stage);
}

} // namespace entwine


/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <pdal/Filter.hpp>
#include <pdal/Reader.hpp>

#include <entwine/types/bounds.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/vector-point-table.hpp>
#include <entwine/util/unique.hpp>

namespace pdal
{
    class StageFactory;
}

namespace entwine
{

class Reprojection;
class Schema;

class ScopedStage
{
public:
    explicit ScopedStage(
            pdal::Stage* stage,
            pdal::StageFactory& stageFactory,
            std::mutex& factoryMutex);

    ~ScopedStage();

    pdal::Stage* get() { return m_stage; }

private:
    pdal::Stage* m_stage;
    pdal::StageFactory& m_stageFactory;
    std::mutex& m_factoryMutex;
};

typedef std::unique_ptr<ScopedStage> UniqueStage;

class Preview
{
public:
    Preview() = default;

    Preview(
            const Bounds& bounds,
            std::size_t numPoints,
            const std::string& srs,
            const std::vector<std::string>& dimNames,
            const Scale* scale,
            const Json::Value& metadata)
        : bounds(bounds)
        , numPoints(numPoints)
        , srs(srs)
        , dimNames(dimNames)
        , scale(maybeClone(scale))
        , metadata(metadata)
    { }

    Bounds bounds;
    std::size_t numPoints = 0;
    std::string srs;
    std::vector<std::string> dimNames;
    std::unique_ptr<Scale> scale;
    Json::Value metadata;
};

class Executor
{
public:
    static Executor& get()
    {
        static Executor e;
        return e;
    }

    static std::mutex& mutex()
    {
        return get().m_mutex;
    }

    // True if this path is recognized as a point cloud file.
    bool good(std::string path) const;

    bool run(pdal::StreamPointTable& table, const Json::Value& pipeline);

    // If available, return the bounds specified in the file header without
    // reading the whole file.
    std::unique_ptr<Preview> preview(
            std::string path,
            const Reprojection* reprojection = nullptr);

    std::string getSrsString(std::string input) const;

    // std::vector<std::string> dims(std::string path) const;

    static std::unique_lock<std::mutex> getLock();

private:
    UniqueStage createReader(std::string path) const;
    std::unique_ptr<Preview> slowPreview(
            std::string path,
            const Reprojection* reprojection) const;

    Executor();
    ~Executor();

    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;

    Reprojection srsFoundOrDefault(
            const pdal::SpatialReference& found,
            const Reprojection& given);

    mutable std::mutex m_mutex;
    std::unique_ptr<pdal::StageFactory> m_stageFactory;
};

} // namespace entwine

